"""
使用vnpy datafeed系统下载2025年全年EOD因子所需数据
包括：日线数据、1分钟、5分钟、60分钟K线数据
数据保存到 MongoDB 数据库（使用vnpy全局配置）

需要配置：
1. 全局配置中设置 datafeed.name = "xt"（或其他支持的数据源）
2. 全局配置中设置数据库连接信息
"""
import os
import sys
from datetime import datetime, timedelta
from typing import List, Set
import logging
from tqdm import tqdm
import time

# 导入vnpy相关模块
try:
    from vnpy.trader.database import get_database, DB_TZ
    from vnpy.trader.constant import Exchange, Interval
    from vnpy.trader.object import BarData, HistoryRequest
    from vnpy.trader.setting import SETTINGS
    from vnpy.trader.utility import extract_vt_symbol
    from vnpy.trader.datafeed import get_datafeed
except ImportError:
    print("请确保vnpy已正确安装")
    sys.exit(1)

# 可选：如果使用xt数据源，可能需要xtquant来获取股票列表
try:
    from xtquant import xtdata
    HAS_XTQUANT = True
except ImportError:
    HAS_XTQUANT = False
    print("警告: 未安装xtquant库，将无法使用xt特有的功能（如获取股票列表）")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class QMTDataDownloader:
    """使用vnpy datafeed系统的数据下载器，支持保存到MongoDB"""
    
    def __init__(self, start_date: str = "20250101", end_date: str = "20251231", 
                 save_to_db: bool = True):
        """
        初始化下载器
        Args:
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            save_to_db: 是否保存到数据库（默认True）
        """
        self.start_date = start_date
        self.end_date = end_date
        self.save_to_db = save_to_db
        
        # 周期映射：字符串周期 -> vnpy Interval
        # 注意：5分钟数据在vnpy中也是MINUTE类型，需要在datafeed层面处理
        self.period_map = {
            '1d': Interval.DAILY,
            '1m': Interval.MINUTE,
            '5m': Interval.MINUTE,  # 5分钟也是MINUTE类型，datafeed会处理
            '60m': Interval.HOUR,
        }
        
        # 初始化datafeed（使用全局配置）
        try:
            self.datafeed = get_datafeed()
            datafeed_name = SETTINGS.get("datafeed.name", "")
            if datafeed_name:
                logger.info(f"数据服务连接成功: {datafeed_name}")
                # 初始化datafeed
                if hasattr(self.datafeed, 'init'):
                    init_result = self.datafeed.init()
                    if init_result:
                        logger.info(f"数据服务初始化成功: {datafeed_name}")
                    else:
                        logger.warning(f"数据服务初始化返回False: {datafeed_name}")
                else:
                    logger.debug(f"数据服务没有init方法: {datafeed_name}")
            else:
                logger.warning("未配置数据服务（datafeed.name），请检查全局配置")
                logger.warning("将使用BaseDatafeed（可能无法获取数据）")
        except Exception as e:
            logger.error(f"数据服务初始化失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            logger.warning("请检查全局配置中的datafeed设置")
            self.datafeed = None
        
        # 初始化数据库（使用全局配置）
        if self.save_to_db:
            try:
                self.database = get_database()
                db_name = SETTINGS.get("database.name", "unknown")
                db_host = SETTINGS.get("database.host", "unknown")
                db_port = SETTINGS.get("database.port", "unknown")
                logger.info(f"数据库连接成功: {db_name} @ {db_host}:{db_port}")
            except Exception as e:
                logger.error(f"数据库连接失败: {e}")
                logger.warning("将只下载数据，不保存到数据库")
                logger.warning("请检查全局配置中的数据库设置")
                self.save_to_db = False
                self.database = None
    
    def get_trading_dates(self) -> List[str]:
        """获取交易日列表"""
        # 注意：这里需要根据实际情况获取交易日历
        # 简化处理：生成所有日期，实际使用时需要过滤非交易日
        start = datetime.strptime(self.start_date, "%Y%m%d")
        end = datetime.strptime(self.end_date, "%Y%m%d")
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        
        logger.info(f"生成日期范围: {self.start_date} 到 {self.end_date}, 共{len(dates)}天")
        return dates
    
    def get_chinext_stock_list(self) -> List[str]:
        """
        获取创业板所有股票列表
        创业板代码范围：300000-301999.SZ
        返回格式：["300001.SZ", "300002.SZ", ...]
        """
        if not HAS_XTQUANT:
            logger.error("需要xtquant库来获取股票列表，请先安装: pip install xtquant")
            return []
        
        try:
            # 先下载板块数据
            logger.info("下载板块数据...")
            xtdata.download_sector_data()
            
            logger.info("获取创业板股票列表...")
            
            # 方法1：从创业板指数获取成分股（最准确）
            try:
                index_symbol = "399006.SZ"  # 创业板指
                stock_list = xtdata.get_stock_list_in_sector(index_symbol, self.start_date)
                if stock_list:
                    logger.info(f"从创业板指数获取到 {len(stock_list)} 只股票")
                    return stock_list
            except Exception as e:
                logger.warning(f"从创业板指数获取成分股失败: {e}")
            
            # 方法2：通过代码范围生成创业板股票列表
            logger.info("通过代码范围生成创业板股票列表...")
            stock_list = []
            
            # 创业板代码范围：300000-300999, 301000-301999
            for code_prefix in [300, 301]:
                for code_suffix in range(1000):
                    stock_code = f"{code_prefix}{code_suffix:03d}.SZ"
                    stock_list.append(stock_code)
            
            logger.info(f"生成创业板股票代码范围: {len(stock_list)} 只")
            logger.warning("注意：生成的代码可能包含已退市或未上市的股票，下载时会自动跳过无效代码")
            
            return stock_list
            
        except Exception as e:
            logger.error(f"获取创业板股票列表失败: {e}")
            return []
    
    def get_stock_list(self) -> List[str]:
        """
        获取股票列表（默认获取创业板）
        可以从指数成分股获取，或者从全市场获取
        """
        # 默认获取创业板股票
        return self.get_chinext_stock_list()
    
    def _parse_stock_code(self, stock_code: str) -> tuple:
        """
        解析股票代码，返回(symbol, exchange)
        Args:
            stock_code: 股票代码，格式如 "600000.SH" 或 "600000.SSE"
        Returns:
            (symbol, exchange) 元组
        """
        if stock_code.endswith('.SH') or stock_code.endswith('.SSE'):
            symbol = stock_code.split('.')[0]
            exchange = Exchange.SSE
        elif stock_code.endswith('.SZ') or stock_code.endswith('.SZSE'):
            symbol = stock_code.split('.')[0]
            exchange = Exchange.SZSE
        else:
            # 尝试解析为vt_symbol格式
            try:
                symbol, exchange_str = extract_vt_symbol(stock_code)
                exchange = Exchange(exchange_str)
            except:
                logger.warning(f"无法解析股票代码格式: {stock_code}")
                return None, None
        return symbol, exchange
    
    def download_stock_data(self, stock_code: str, period: str, 
                           start_time: str = None, end_time: str = None) -> bool:
        """
        使用vnpy datafeed下载单只股票的历史数据并保存到数据库
        Args:
            stock_code: 股票代码，格式如 "600000.SH" 或 "600000.SSE"
            period: 周期，如 "1d", "1m", "5m", "60m"
            start_time: 开始时间，格式YYYYMMDD
            end_time: 结束时间，格式YYYYMMDD
        Returns:
            是否成功
        """
        if not self.datafeed:
            logger.error("数据服务未初始化，无法下载数据")
            return False
        
        try:
            if start_time is None:
                start_time = self.start_date
            if end_time is None:
                end_time = self.end_date
            
            # 解析股票代码
            symbol, exchange = self._parse_stock_code(stock_code)
            if symbol is None or exchange is None:
                logger.warning(f"无法解析股票代码: {stock_code}")
                return False
            
            # 获取周期对应的Interval
            interval = self.period_map.get(period, Interval.MINUTE)
            
            # 转换时间格式
            start_dt = datetime.strptime(start_time, "%Y%m%d")
            start_dt = start_dt.replace(tzinfo=DB_TZ)
            
            end_dt = datetime.strptime(end_time, "%Y%m%d")
            end_dt = end_dt.replace(hour=23, minute=59, second=59, tzinfo=DB_TZ)
            
            # 对于5分钟数据，需要特殊处理
            # 如果datafeed支持自定义分钟数，可以在HistoryRequest中指定
            # 否则可能需要通过其他方式处理
            if period == '5m':
                # 某些datafeed可能需要特殊处理5分钟数据
                # 这里先尝试使用MINUTE，如果datafeed支持，会自动处理
                interval = Interval.MINUTE
            
            # 构建HistoryRequest
            req = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                start=start_dt,
                end=end_dt,
                interval=interval
            )
            
            # 如果使用xt数据源，可能需要先下载数据到本地
            # 某些datafeed（如vnpy_xt）可能需要先调用download_history_data
            if HAS_XTQUANT and SETTINGS.get("datafeed.name", "") == "xt":
                try:
                    # 转换stock_code格式为xt格式（如果需要）
                    if stock_code.endswith('.SSE'):
                        xt_code = stock_code.replace('.SSE', '.SH')
                    elif stock_code.endswith('.SZSE'):
                        xt_code = stock_code.replace('.SZSE', '.SZ')
                    else:
                        xt_code = stock_code
                    
                    logger.debug(f"预下载数据到本地: {xt_code} {period} ({start_time} - {end_time})")
                    
                    # xt需要先下载数据到本地
                    logger.info(f"正在下载数据到xt本地: {xt_code} {period} ({start_time} - {end_time})")
                    try:
                        result = xtdata.download_history_data(
                            xt_code,
                            period=period,
                            start_time=start_time,
                            end_time=end_time,
                            incrementally=True
                        )
                        logger.debug(f"download_history_data返回: {result}")
                    except Exception as e:
                        logger.warning(f"download_history_data异常: {e}")
                    
                    # 增加等待时间，确保数据下载完成
                    logger.info("等待数据下载完成...")
                    time.sleep(2.0)  # 增加等待时间到2秒
                    logger.debug(f"预下载完成: {xt_code} {period}")
                except Exception as e:
                    logger.warning(f"预下载数据到本地失败 {stock_code} {period}: {e}")
                    # 即使预下载失败，也尝试查询（可能数据已存在）
            
            # 通过datafeed查询数据
            logger.info(f"查询数据: {symbol}.{exchange.value} {interval} ({start_dt} - {end_dt})")
            logger.debug(f"HistoryRequest: symbol={symbol}, exchange={exchange}, start={start_dt}, end={end_dt}, interval={interval}")
            
            bars = []
            
            # 方法1：尝试通过datafeed查询
            try:
                bars = self.datafeed.query_bar_history(req)
                logger.debug(f"datafeed.query_bar_history 返回: {len(bars) if bars else 0} 条数据")
            except Exception as e:
                logger.warning(f"通过datafeed查询数据失败: {e}")
                logger.debug("将尝试直接从xt获取数据")
            
            # 方法2：如果datafeed返回空，且使用xt数据源，直接从xt获取数据
            if not bars and HAS_XTQUANT and SETTINGS.get("datafeed.name", "") == "xt":
                try:
                    if stock_code.endswith('.SSE'):
                        xt_code = stock_code.replace('.SSE', '.SH')
                    elif stock_code.endswith('.SZSE'):
                        xt_code = stock_code.replace('.SZSE', '.SZ')
                    else:
                        xt_code = stock_code
                    
                    logger.info(f"直接从xt获取数据: {xt_code} {period}")
                    
                    # 根据xt文档，get_market_data_ex应该使用count=-1获取所有本地数据
                    # 然后手动过滤日期范围
                    import pandas as pd  # 确保pd已导入
                    
                    try:
                        # 方法1：使用count=-1获取所有本地数据（推荐方式）
                        logger.info("使用count=-1获取所有本地数据...")
                        xt_data = xtdata.get_market_data_ex(
                            field_list=[],  # 空列表表示获取所有字段
                            stock_list=[xt_code],
                            period=period,
                            count=-1  # -1表示获取所有本地数据
                        )
                        logger.debug(f"get_market_data_ex(count=-1)返回类型: {type(xt_data)}")
                        
                        # 如果返回的是字典，提取对应股票的数据
                        if isinstance(xt_data, dict):
                            if xt_code in xt_data:
                                data = xt_data[xt_code]
                                logger.debug(f"提取的数据类型: {type(data)}, 是否为空: {data.empty if isinstance(data, pd.DataFrame) else 'N/A'}")
                                
                                # 如果是DataFrame，需要按日期过滤
                                if isinstance(data, pd.DataFrame) and not data.empty:
                                    # 过滤日期范围
                                    if 'time' in data.columns:
                                        # time列可能是日期字符串或时间戳
                                        try:
                                            # 尝试转换为日期
                                            data['date'] = pd.to_datetime(data['time'], errors='coerce')
                                            # 过滤日期范围
                                            start_dt_filter = datetime.strptime(start_time, "%Y%m%d")
                                            end_dt_filter = datetime.strptime(end_time, "%Y%m%d")
                                            mask = (data['date'] >= start_dt_filter) & (data['date'] <= end_dt_filter)
                                            data = data[mask]
                                            logger.info(f"过滤后数据量: {len(data)} 条")
                                        except Exception as e:
                                            logger.warning(f"日期过滤失败: {e}")
                                    
                                    # 更新xt_data字典
                                    xt_data[xt_code] = data
                                elif isinstance(data, pd.DataFrame) and data.empty:
                                    logger.warning(f"xt本地数据为空: {xt_code} {period}")
                            else:
                                logger.warning(f"xt返回的字典中没有 {xt_code}")
                                xt_data = None
                    except Exception as e:
                        logger.error(f"get_market_data_ex失败: {e}")
                        import traceback
                        logger.debug(traceback.format_exc())
                        xt_data = None
                    
                    if xt_data and xt_code in xt_data:
                        # 转换xt数据为BarData
                        import pandas as pd
                        data = xt_data[xt_code]
                        
                        logger.info(f"xt返回数据类型: {type(data)}")
                        if isinstance(data, pd.DataFrame):
                            logger.info(f"DataFrame形状: {data.shape}, 列: {data.columns.tolist()}")
                            if not data.empty:
                                logger.info(f"DataFrame前5行:\n{data.head()}")
                                
                                # DataFrame格式
                                for idx, row in data.iterrows():
                                    try:
                                        # 转换时间
                                        if isinstance(idx, pd.Timestamp):
                                            dt = idx.to_pydatetime()
                                        else:
                                            dt = datetime.strptime(str(idx), "%Y%m%d") if len(str(idx)) == 8 else datetime.fromtimestamp(idx)
                                        
                                        if dt.tzinfo is None:
                                            dt = dt.replace(tzinfo=DB_TZ)
                                        dt = dt.astimezone(DB_TZ).replace(tzinfo=None)
                                        
                                        # 创建BarData
                                        bar = BarData(
                                            symbol=symbol,
                                            exchange=exchange,
                                            datetime=dt,
                                            interval=interval,
                                            open_price=float(row.get('open', 0)),
                                            high_price=float(row.get('high', 0)),
                                            low_price=float(row.get('low', 0)),
                                            close_price=float(row.get('close', 0)),
                                            volume=float(row.get('volume', 0)),
                                            turnover=float(row.get('amount', 0)),
                                            gateway_name="xt"
                                        )
                                        bars.append(bar)
                                    except Exception as e:
                                        logger.debug(f"转换数据行失败: {e}")
                                        continue
                                
                                logger.info(f"从xt直接获取并转换了 {len(bars)} 条数据")
                            else:
                                logger.warning("DataFrame为空！")
                        elif isinstance(data, dict):
                            logger.info(f"返回的是字典格式，键: {list(data.keys())[:10]}")
                            logger.info(f"字典示例: {str(data)[:200]}")
                            logger.warning("字典格式暂不支持，需要手动转换")
                        else:
                            logger.warning(f"未知的数据格式: {type(data)}")
                    else:
                        logger.warning(f"xt本地也没有数据: {xt_code} {period}")
                except Exception as e:
                    logger.error(f"直接从xt获取数据失败: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
            
            if not bars:
                logger.warning(f"未获取到数据: {stock_code} {period} (symbol={symbol}, exchange={exchange.value}, interval={interval})")
                logger.debug(f"请求详情: start={start_dt}, end={end_dt}, period={period}")
                return False
            
            logger.debug(f"成功获取 {len(bars)} 条数据: {stock_code} {period}")
            
            # 保存到数据库
            if self.save_to_db and self.database:
                try:
                    self.database.save_bar_data(bars, stream=True)
                    logger.debug(f"保存 {stock_code} {period} 数据到数据库: {len(bars)}条")
                except Exception as e:
                    logger.warning(f"保存 {stock_code} {period} 到数据库失败: {e}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"下载 {stock_code} {period} 数据失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
    
    def download_all_data(self, stock_list: List[str] = None, 
                         periods: List[str] = None,
                         filter_invalid: bool = True) -> dict:
        """
        下载所有数据
        Args:
            stock_list: 股票列表，如果为None则自动获取
            periods: 需要下载的周期列表，如 ['1d', '1m', '5m', '60m']，如果为None则下载所有周期
            filter_invalid: 是否过滤无效股票代码（默认True）
        Returns:
            下载结果统计
        """
        if stock_list is None:
            stock_list = self.get_stock_list()
        
        if periods is None:
            # 默认下载所有周期
            periods = ['1d', '1m', '5m', '60m']
        
        if not stock_list:
            logger.error("股票列表为空，无法下载")
            return {}
        
        # 如果股票列表很大，提示用户
        if len(stock_list) > 1000:
            logger.warning(f"股票列表较大({len(stock_list)}只)，下载可能需要较长时间")
            logger.info("建议：可以先测试少量股票，确认无误后再下载全部")
        
        logger.info(f"开始下载数据: {len(stock_list)}只股票, {len(periods)}个周期")
        
        results = {
            'total': len(stock_list) * len(periods),
            'success': 0,
            'failed': 0,
            'failed_items': []
        }
        
        # 遍历所有股票和周期
        with tqdm(total=results['total'], desc="下载进度") as pbar:
            for stock_code in stock_list:
                for period in periods:
                    success = self.download_stock_data(
                        stock_code, 
                        period, 
                        self.start_date, 
                        self.end_date
                    )
                    
                    if success:
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                        results['failed_items'].append(f"{stock_code}_{period}")
                    
                    pbar.update(1)
                    pbar.set_postfix({
                        '成功': results['success'],
                        '失败': results['failed']
                    })
        
        return results
    
    def download_index_data(self, index_codes: List[str] = None):
        """
        下载指数数据
        Args:
            index_codes: 指数代码列表，如 ["000300.SH", "399001.SZ"]
        """
        if index_codes is None:
            # 默认下载主要指数
            index_codes = [
                "000300.SH",  # 沪深300
                "000001.SH",  # 上证指数
                "399001.SZ",  # 深证成指
                "399006.SZ",  # 创业板指
            ]
        
        logger.info(f"开始下载指数数据: {len(index_codes)}个指数")
        
        periods = ['1d', '60m']  # 指数主要下载日线和60分钟数据
        
        for index_code in index_codes:
            for period in periods:
                try:
                    logger.info(f"下载 {index_code} {period} 数据...")
                    self.download_stock_data(
                        index_code,
                        period,
                        self.start_date,
                        self.end_date
                    )
                except Exception as e:
                    logger.error(f"下载指数 {index_code} {period} 失败: {e}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="下载2025年全年EOD因子数据")
    parser.add_argument("--start_date", type=str, default="20250101", 
                       help="开始日期，格式YYYYMMDD")
    parser.add_argument("--end_date", type=str, default="20251231",
                       help="结束日期，格式YYYYMMDD")
    parser.add_argument("--stock_list", type=str, nargs='+', default=None,
                       help="指定股票列表，如: 600000.SH 000001.SZ（如果指定，则下载指定股票）")
    parser.add_argument("--all_stocks", action='store_true',
                       help="下载所有股票（默认只下载创业板股票）")
    parser.add_argument("--periods", type=str, nargs='+', 
                       default=['1d', '1m', '5m', '60m'],
                       help="下载周期，默认: 1d 1m 5m 60m")
    parser.add_argument("--download_index", action='store_true',
                       help="是否下载指数数据")
    parser.add_argument("--debug", action='store_true',
                       help="启用调试模式（显示详细日志）")
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # 创建下载器
    downloader = QMTDataDownloader(
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    # 下载指数数据（如果需要）
    if args.download_index:
        downloader.download_index_data()
    
    # 下载股票数据
    stock_list = args.stock_list
    if stock_list:
        logger.info(f"使用指定的股票列表: {len(stock_list)}只")
    else:
        if args.all_stocks:
            logger.info("获取所有股票列表...")
            stock_list = downloader.get_stock_list()
        else:
            # 默认下载创业板股票
            logger.info("自动获取创业板股票列表（默认行为）...")
            stock_list = downloader.get_chinext_stock_list()
    
    # 执行下载
    results = downloader.download_all_data(
        stock_list=stock_list,
        periods=args.periods,
        filter_invalid=True
    )
    
    # 输出结果
    logger.info("=" * 50)
    logger.info("下载完成！")
    logger.info(f"总计: {results['total']} 项")
    logger.info(f"成功: {results['success']} 项")
    logger.info(f"失败: {results['failed']} 项")
    logger.info("=" * 50)
    logger.info("数据存储说明:")
    datafeed_name = SETTINGS.get("datafeed.name", "unknown")
    logger.info(f"1. 数据服务: {datafeed_name}")
    if downloader.save_to_db:
        logger.info("2. 数据已保存到数据库")
        logger.info(f"   - 数据库类型: {SETTINGS.get('database.name', 'unknown')}")
        logger.info(f"   - 服务器地址: {SETTINGS.get('database.host', 'unknown')}:{SETTINGS.get('database.port', 'unknown')}")
        logger.info(f"   - 数据库实例: {SETTINGS.get('database.database', 'unknown')}")
    else:
        logger.info("2. 数据未保存到数据库（数据库连接失败或未启用）")
        logger.warning("   请检查全局配置中的数据库设置")
    
    if results['failed_items']:
        logger.warning(f"失败项数量: {len(results['failed_items'])}")
        if len(results['failed_items']) <= 20:
            logger.warning(f"失败项列表: {results['failed_items']}")
        else:
            logger.warning(f"前20个失败项: {results['failed_items'][:20]}")


if __name__ == "__main__":
    main()

