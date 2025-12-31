# -*- coding: utf-8 -*-
"""测试数据下载"""
import sys
import logging

# 设置UTF-8编码
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from scripts.download_eod_data_2025 import QMTDataDownloader

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True  # 强制重新配置
)

# 设置所有相关logger为DEBUG级别
logging.getLogger('scripts.download_eod_data_2025').setLevel(logging.DEBUG)
logging.getLogger('__main__').setLevel(logging.DEBUG)

if __name__ == "__main__":
    print("=" * 50)
    print("测试数据下载")
    print("=" * 50)
    
    # 创建下载器（使用2023年数据，更可能已有数据）
    downloader = QMTDataDownloader(
        start_date="20230101",
        end_date="20231231"
    )
    
    # 先测试xt是否能直接获取数据
    print("\n测试xt直接获取数据...")
    try:
        from xtquant import xtdata
        import pandas as pd
        
        # 测试获取数据
        print("尝试获取 000001.SZ 的日线数据...")
        data = xtdata.get_market_data_ex(
            field_list=['time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
            stock_list=['000001.SZ'],
            period='1d',
            start_time='20230101',
            end_time='20231231'
        )
        print(f"返回类型: {type(data)}")
        if isinstance(data, dict):
            print(f"字典键: {list(data.keys())}")
            if '000001.SZ' in data:
                df = data['000001.SZ']
                print(f"DataFrame形状: {df.shape if hasattr(df, 'shape') else 'N/A'}")
                if hasattr(df, 'head'):
                    print(f"前5行:\n{df.head()}")
        elif isinstance(data, pd.DataFrame):
            print(f"DataFrame形状: {data.shape}")
            print(f"前5行:\n{data.head()}")
        else:
            print(f"数据内容: {str(data)[:200]}")
    except Exception as e:
        print(f"xt直接获取数据失败: {e}")
        import traceback
        traceback.print_exc()
    
    # 测试下载单个股票
    print("\n测试下载 000001.SZ 的日线数据（2023年）...")
    success = downloader.download_stock_data(
        "000001.SZ",
        "1d",
        "20230101",
        "20231231"
    )
    
    if success:
        print("✓ 下载成功！")
    else:
        print("✗ 下载失败，请查看上面的日志信息")

