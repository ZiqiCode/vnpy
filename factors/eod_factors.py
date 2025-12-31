"""
EOD因子提取模块
从 eod_sig.py 中提取的核心因子计算逻辑
"""
import pandas as pd
import numpy as np
import statsmodels.api as sm
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# 全局参数
FM_ZERO = 1e-12


def get_date_period(cur_date: int, lag: int, all_date_list: List[int]) -> List[int]:
    """获取日期区间"""
    cur_idx = all_date_list.index(cur_date)
    pre_idx = cur_idx - lag + 1
    if pre_idx < 0:
        pre_idx = 0
    pick_list = all_date_list[pre_idx:(cur_idx + 1)]
    return pick_list


def calc_reg_normal(y: np.ndarray, x: np.ndarray) -> np.ndarray:
    """计算回归残差"""
    x = sm.add_constant(x)
    model = sm.OLS(y, x).fit()
    return model.resid


def cr_reg_resr2(x1_: np.ndarray, x2_: np.ndarray):
    """计算回归的R2和残差"""
    x1_ = np.array(x1_)
    x2_ = np.array(x2_)
    d = x1_.shape[1]
    x1_mean = x1_.sum(1) / d
    x2_mean = x2_.sum(1) / d
    x1_error = np.transpose(np.transpose(x1_, (1, 0)) - x1_mean, (1, 0))
    x2_error = np.transpose(np.transpose(x2_, (1, 0)) - x2_mean, (1, 0))
    ssm1 = (x1_error * x2_error).sum(1)
    ssm2 = (x2_error ** 2).sum(1) + FM_ZERO
    ss_tot = (x1_error ** 2).sum(1) + FM_ZERO
    beta1 = ssm1 / ssm2
    beta0 = x1_mean - beta1 * x2_mean
    ss_res = np.transpose(np.transpose(x1_, (1, 0)) - beta0 - np.transpose(x2_, (1, 0)) * beta1, (1, 0))
    ss_res_new = np.transpose(np.transpose(x1_, (1, 0)) - np.transpose(x2_, (1, 0)) * beta1, (1, 0))
    r2 = 1 - (ss_res ** 2).sum(1) / ss_tot
    return beta0, beta1, ss_res, ss_res_new, r2


def calc_stat(resid1: np.ndarray, resid2: np.ndarray, lag: int) -> np.ndarray:
    """计算统计量"""
    resid = (resid1 - resid2) * 10000
    b = np.std(resid, axis=1)
    b[np.isnan(b)] = np.nan
    b[b == 0] = np.nan
    stat = np.mean(resid, axis=1) / b * np.power(lag, 0.5)
    stat[np.isnan(stat)] = 0
    return stat


class EODFactorCalculator:
    """EOD因子计算器"""
    
    def __init__(self, complete_tag: float = 0.6):
        """
        初始化因子计算器
        Args:
            complete_tag: 完整度标签，用于过滤交易时间不够的股票
        """
        self.complete_tag = complete_tag
    
    def calculate_ext1_reversal(self, daily_data: pd.DataFrame, bar_data: pd.DataFrame, 
                                 cur_date: int, lag: int = 20) -> Optional[pd.DataFrame]:
        """
        计算日度理想反转因子 (ext1)
        基于平均成交金额的高低分组收益率差异
        """
        try:
            # 合并日线和分钟数据
            daily_subset = daily_data[['tdate', 'id', 'amount', 'tyield']].copy()
            bar_subset = bar_data[['tdate', 'id', 'trdcount']].copy()
            
            # 按日期和股票汇总交易次数
            bar_sum = bar_subset.groupby(['tdate', 'id'], as_index=False)['trdcount'].sum()
            
            # 合并数据
            res = pd.merge(daily_subset, bar_sum, on=['tdate', 'id'])
            res = res[res['trdcount'] != 0]
            
            # 剔除交易时间不够的股票
            r1 = res.groupby(['id'], as_index=False)['tdate'].count()
            r1 = r1[r1['tdate'] >= lag * self.complete_tag]
            res = res[res['id'].isin(r1['id'])]
            
            if len(res) == 0:
                return None
            
            # 计算平均成交金额
            res['avg_amt'] = res['amount'] / res['trdcount']
            
            # 按股票计算中位数作为分组阈值
            r1 = res.groupby(['id'], as_index=False)['avg_amt'].median()
            r1.columns = ['id', 'tag']
            res = pd.merge(res, r1, on='id')
            
            # 分组
            res['dir'] = 'high'
            res.loc[res['avg_amt'] <= res['tag'], 'dir'] = 'low'
            
            # 计算高低组的收益率差异
            r1 = res.groupby(['id', 'dir'], as_index=False)['tyield'].sum()
            r1 = r1.pivot_table(index='id', columns='dir', values='tyield')
            r1.fillna(0, inplace=True)
            r1['rev_d'] = r1['high'] - r1['low']
            r1['id'] = r1.index
            r1.reset_index(drop=True, inplace=True)
            
            return r1[['id', 'rev_d']]
        except Exception as e:
            logger.error(f"计算ext1因子失败: {e}")
            return None
    
    def calculate_ext3_smart_money(self, daily_data: pd.DataFrame, minute_data: pd.DataFrame,
                                   cur_date: int, lag: int = 10) -> Optional[pd.DataFrame]:
        """
        计算聪明钱因子 (ext3)
        基于成交量加权的VWAP信号
        """
        try:
            split = 0.2
            daily_subset = daily_data[['tdate', 'id', 'adjfactor']].copy()
            minute_subset = minute_data.copy()
            
            # 合并数据
            res = pd.merge(daily_subset, minute_subset, on=['tdate', 'id'])
            
            # 剔除交易时间不够的股票
            r1 = res.groupby(['id'], as_index=False)['tdate'].nunique()
            r1 = r1[r1['tdate'] >= lag * self.complete_tag]
            res = res[res['id'].isin(r1['id'])]
            
            if len(res) == 0:
                return None
            
            # 成交量复权
            res['volume'] = res['volume'] / res['adjfactor']
            res['absr'] = abs(res['close'] / res['pclose'] - 1)
            
            # 计算多个信号
            res['s1'] = res['volume']
            res['rankV'] = res.groupby(['id'], as_index=False)['volume'].rank(method='dense')
            res['rankR'] = res.groupby(['id'], as_index=False)['absr'].rank(method='dense')
            res['s2'] = res['rankR'] + res['rankV']
            
            res.loc[res['volume'] <= 0, 'volume'] = np.nan
            res['s3'] = res['absr'] / np.log(res['volume'])
            res['s4'] = res['absr'] / np.power(res['volume'], -0.5)
            res['s5'] = res['absr'] / np.power(res['volume'], -0.25)
            res['s6'] = res['absr'] / np.power(res['volume'], -0.1)
            res['s7'] = res['absr'] / np.power(res['volume'], 0.1)
            res['s8'] = res['absr'] / np.power(res['volume'], 0.25)
            res['s9'] = res['absr'] / np.power(res['volume'], 0.5)
            res['s10'] = res['absr'] / np.power(res['volume'], 0.7)
            res.fillna(0, inplace=True)
            
            # 计算VWAP
            vwap_all = res.groupby(['id'], as_index=False)[['volume', 'amount']].sum()
            vwap_all.columns = ['id', 'totalV', 'totalA']
            vwap_all['vwap'] = vwap_all['totalA'] / vwap_all['totalV']
            res = pd.merge(res, vwap_all[['id', 'totalV']], on='id')
            res['vp'] = res['volume'] / res['totalV']
            
            # 计算各信号的VWAP
            final_set = []
            idx_list = np.arange(1, 11)
            for cur_idx in idx_list:
                cur_sig = f's{cur_idx}'
                res.sort_values(by=['id', cur_sig], ascending=[True, False], inplace=True)
                res['split'] = res.groupby(['id'], as_index=False)['vp'].cumsum()
                temp_set = res[res['split'] <= split]
                temp_set = temp_set.groupby(['id'], as_index=False)[['volume', 'amount']].sum()
                temp_set['vwapS'] = temp_set['amount'] / temp_set['volume']
                temp_set['type'] = cur_sig
                final_set.append(temp_set[['id', 'type', 'vwapS']])
            
            if len(final_set) == 0:
                return None
            final_set = pd.concat(final_set, ignore_index=True)
            final_set = pd.merge(final_set, vwap_all[['id', 'vwap']], on='id')
            final_set['sig'] = final_set['vwapS'] / final_set['vwap']
            final_set = final_set.pivot_table(index='id', columns='type', values='sig')
            final_set['id'] = final_set.index
            final_set.reset_index(drop=True, inplace=True)
            
            return final_set
        except Exception as e:
            logger.error(f"计算ext3因子失败: {e}")
            return None
    
    def calculate_ext6_amplitude(self, daily_data: pd.DataFrame, cur_date: int, 
                                 lag: int = 20) -> Optional[pd.DataFrame]:
        """
        计算理想振幅因子 (ext6)
        基于价格分位数的振幅差异
        """
        try:
            split_list = [0.2, 0.25, 0.3]
            
            # 数据清洗
            day_set = daily_data.copy()
            day_set = day_set[day_set['volume'] > 0]
            day_set = day_set[~((day_set['open'] == day_set['high']) & 
                               (day_set['high'] == day_set['low']) & 
                               (day_set['low'] == day_set['close']))]
            
            # 剔除交易时间不够的股票
            r1 = day_set.groupby(['id'], as_index=False)['tdate'].nunique()
            r1 = r1[r1['tdate'] >= lag * self.complete_tag]
            day_set = day_set[day_set['id'].isin(r1['id'])]
            
            if len(day_set) == 0:
                return None
            
            day_set['amplitude'] = day_set['high'] / day_set['low'] - 1
            day_set['adjclose'] = day_set['close'] * day_set['adjfactor']
            
            final_set = None
            for cur_split in split_list:
                res = day_set.groupby(['id'], as_index=False)['adjclose'].quantile([cur_split, 1 - cur_split])
                res.reset_index(inplace=True)
                res.loc[res['level_1'] == cur_split, 'level_1'] = 'v_low'
                res.loc[res['level_1'] == (1 - cur_split), 'level_1'] = 'v_high'
                res = res.pivot_table(index='id', columns='level_1', values='adjclose')
                res.reset_index(inplace=True)
                
                temp_set = pd.merge(day_set, res, on='id')
                v_high = temp_set[temp_set['adjclose'] >= temp_set['v_high']].groupby(
                    ['id'], as_index=False)[['amplitude', 'to']].mean()
                v_low = temp_set[temp_set['adjclose'] <= temp_set['v_low']].groupby(
                    ['id'], as_index=False)[['amplitude', 'to']].mean()
                
                v_high.columns = ['id', 'high', 'high_to']
                v_low.columns = ['id', 'low', 'low_to']
                temp_set = pd.merge(v_high, v_low, on='id')
                temp_set[f'amplitude_{int(cur_split * 100)}'] = temp_set['high'] - temp_set['low']
                temp_set[f'turnover_{int(cur_split * 100)}'] = temp_set['high_to'] - temp_set['low_to']
                
                del temp_set['high'], temp_set['low'], temp_set['high_to'], temp_set['low_to']
                
                if final_set is None:
                    final_set = temp_set
                else:
                    final_set = pd.merge(final_set, temp_set, on='id')
            
            return final_set
        except Exception as e:
            logger.error(f"计算ext6因子失败: {e}")
            return None
    
    def calculate_ext15_anchor_reversal(self, daily_data: pd.DataFrame, cur_date: int,
                                        lag: int = 20) -> Optional[pd.DataFrame]:
        """
        计算锚定反转因子 (ext15)
        基于价格锚点的反转信号
        """
        try:
            N_long = 60
            N_short = 10
            
            # 获取长期数据
            day_set = daily_data.copy()
            
            # 剔除交易时间不够的股票
            r1 = day_set.groupby(['id'], as_index=False)['tdate'].nunique()
            r1 = r1[r1['tdate'] >= lag * self.complete_tag]
            day_set = day_set[day_set['id'].isin(r1['id'])]
            
            if len(day_set) == 0:
                return None
            
            # 计算短期收益率
            day_set['lnr'] = np.log(day_set['tyield'] + 1)
            temp_set = day_set.groupby(['id'], as_index=False)['lnr'].sum()
            temp_set['tag'] = np.power(np.exp(1), temp_set['lnr']) - 1
            
            # 计算价格区间
            day_set['close'] = day_set['close'] * day_set['adjfactor']
            res = day_set.groupby(['id'], as_index=False)['close'].agg(['min', 'max'])
            res.reset_index(inplace=True)
            res.columns = ['id', 'min_p', 'max_p']
            
            # 获取当前价格
            cur_price = day_set[day_set['tdate'] == cur_date][['id', 'close']].copy()
            if len(cur_price) == 0:
                return None
            res = pd.merge(res, cur_price, on='id', how='left')
            res = pd.merge(res, temp_set[['id', 'tag']], on='id')
            
            # 计算锚定反转
            res['anchor_rev'] = res['close'] / res['min_p'] - 1
            idx = res['tag'] <= 0
            res.loc[idx, 'anchor_rev'] = res.loc[idx, 'close'] / res.loc[idx, 'max_p'] - 1
            
            return res[['id', 'anchor_rev']]
        except Exception as e:
            logger.error(f"计算ext15因子失败: {e}")
            return None


def calculate_all_factors(daily_data: pd.DataFrame, bar_data: Dict[str, pd.DataFrame],
                         cur_date: int, lag: int = 20) -> pd.DataFrame:
    """
    计算所有因子并合并
    Args:
        daily_data: 日线数据
        bar_data: 分钟数据字典，key为周期名称（如'1m', '5m', '60m'）
        cur_date: 当前日期
        lag: 回看期数
    Returns:
        合并后的因子DataFrame
    """
    calculator = EODFactorCalculator()
    factors = []
    
    # 计算ext1因子（需要60分钟数据）
    if '60m' in bar_data:
        ext1 = calculator.calculate_ext1_reversal(daily_data, bar_data['60m'], cur_date, lag)
        if ext1 is not None:
            factors.append(ext1)
    
    # 计算ext3因子（需要1分钟数据）
    if '1m' in bar_data:
        ext3 = calculator.calculate_ext3_smart_money(daily_data, bar_data['1m'], cur_date, lag)
        if ext3 is not None:
            factors.append(ext3)
    
    # 计算ext6因子
    ext6 = calculator.calculate_ext6_amplitude(daily_data, cur_date, lag)
    if ext6 is not None:
        factors.append(ext6)
    
    # 计算ext15因子
    ext15 = calculator.calculate_ext15_anchor_reversal(daily_data, cur_date, lag)
    if ext15 is not None:
        factors.append(ext15)
    
    # 合并所有因子
    if len(factors) == 0:
        return pd.DataFrame()
    
    result = factors[0]
    for factor in factors[1:]:
        result = pd.merge(result, factor, on='id', how='outer')
    
    return result

