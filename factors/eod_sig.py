import util_date
import util_db
import util_other
import cfg
import pandas as pd
import os
from joblib import Parallel, delayed
import pyarrow.feather as fe
import math
import numpy as np
import statsmodels.api as sm
import time
from argparse import ArgumentParser 
import logging
import eod_mschars

param_verbose = 3
fm_zero = 1e-12
debug = False
reproduce = False

def debug_output(upper_dir, date, file_name, data_df):
    if not debug:
        return

    sub_dir = os.path.join(cfg.debug_output_dir, upper_dir, "{}".format(date))
    os.makedirs(sub_dir, exist_ok=True)
    out_file = os.path.join(sub_dir, file_name)
    fe.write_feather(data_df, out_file) 

def debug_load(upper_dir, date, file_name):
    debug_file = os.path.join(cfg.debug_output_dir, upper_dir, "{}".format(date), file_name)
    return fe.read_feather(debug_file)

def get_date_period(cur_date, lag, all_date_list):
	cur_idx = all_date_list.index(cur_date)
	pre_idx = cur_idx-lag+1
	if pre_idx<0:
		pre_idx = 0
	pick_list = all_date_list[pre_idx:(cur_idx+1)]
	return pick_list

def process_sig_ext1_main(cur_date, lag, all_date_list, out_folder):
    """
    input: 
        $alpha_root/prod.stock.daily/
        $feats_root/stock_basic_sixtyminute/
    output:
        $output_dir/eod.sig.ext1/basic.%cur_date.csv
    """

    complete_tag = 0.6
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    bar_folder = '%s/stock_basic_sixtyminute/'%(cfg.param_path_src_minbar_file)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return

    pick_list = get_date_period(cur_date, lag, all_date_list)
    day_set = []
    bar_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            logging.error("process_sig_ext1_main missing input file: {}".format(cur_file))
            return
        res = fe.read_feather(cur_file)
        day_set.append(res[['tdate','id','amount','tyield']])

        cur_file = os.path.join(bar_folder, "stock_basic_sixtyminute.{}.csv".format(cur_pick))
        if not os.path.exists(cur_file):
            logging.error("process_sig_ext1_main missing input file: {}".format(cur_file))
            return
        res = pd.read_csv(cur_file)
        res.rename(columns={"min":"bar", "dealcnt":"trdcount"}, inplace=True)
        res["tdate"] = int(cur_pick)
        bar_set.append(res[['tdate','id','bar','trdcount']])

    day_set = pd.DataFrame().append(day_set)
    bar_set = pd.DataFrame().append(bar_set)
    bar_set = bar_set.groupby(['tdate','id'],as_index=False)[['trdcount']].sum()
    res = pd.merge(day_set, bar_set, on=['tdate','id'])
    res = res[res['trdcount']!=0]
    #剔除交易时间不够的股票
    r1 = res.groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    if len(res)==0:
        return

    res['avg_amt'] = res['amount']/res['trdcount']
    r1 = res.groupby(['id'],as_index=False)[['avg_amt']].median()
    r1.columns = ['id','tag']
    res = pd.merge(res, r1, on=['id'])
    res['dir'] = 'high'
    res.loc[res['avg_amt']<=res['tag'], 'dir'] = 'low'
    r1 = res.groupby(['id','dir'],as_index=False)[['tyield']].sum()
    r1 = r1.pivot_table(index='id',columns='dir',values='tyield')
    r1.fillna(0, inplace=True)
    r1['rev_d'] = r1['high']-r1['low']
    r1['id'] = r1.index
    r1.to_csv(out_file, index=False)
	
def process_sig_ext1(begin_date, end_date, output_dir, job_num):
    """
    input: 
    output: 
        $output_dir/eod.sig.ext1/
    """
    out_folder = os.path.join(output_dir, 'eod.sig.ext1')
    os.makedirs(out_folder, exist_ok=True)

    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext1')
    if job_num==0:
        for cur_date in date_list:
            process_sig_ext1_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext1_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext2_main(cur_date, cur_stock, div, trans_folder):
    stock_id = int(cur_stock.replace('SH', '').replace('SZ', ''))
    cur_file = '%s/%s/%s.feather'%(trans_folder, cur_date, cur_stock)
    if not os.path.exists(cur_file):
        return None
    res = fe.read_feather(cur_file)
    res['amt'] = res['price']*res['vol']
    res = res[res['amt']>0]
    if len(res)<div:
        return None
    res = res['amt'].quantile(np.arange(1,div)/div)
    res = pd.DataFrame(res)
    res.columns = ['tag']
    res['pos'] = (res.index*div).astype(int)
    res['id'] = stock_id
    return res

def process_sig_ext2_init(begin_date, end_date, output_dir, job_num, lag):
    """
    date range:
        [begin_date - lag, end_date]
    input:
        $cache_root/ori.trans/ 
    output:
        $output_dir/eod.sig.ext2/
    """
    trans_folder = '%s/ori.trans/' % (cfg.param_path_src_ori_file)
    out_folder = '%s/eod.sig.ext2/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    prev_date_list = get_date_period(begin_date, lag, all_date_list)
    begin_date = prev_date_list[0]
    date_list = [x for x in all_date_list if x>=begin_date]

    all_stock = util_other.get_all_stock()
    all_stock = all_stock[~all_stock['oid2'].str.startswith('BJ')]
    div = 16
    
    logging.info('process sig ext2 init')
    for cur_date in date_list:
        out_file = '%s/init.%s.csv'%(out_folder, cur_date)
        if os.path.exists(out_file):
            continue
        
        stock_list = all_stock.loc[(all_stock['act_date']<=cur_date) & (all_stock['del_date']>=cur_date),'oid2']

        final_set = []
        if job_num==0:
            #stock_list = stock_list[89:91]
            #stock_list = ['SZ300529']
            for cur_stock in stock_list:
                print(cur_stock)
                process_sig_ext2_main(cur_date, cur_stock, div, trans_folder)
                # todo: bug? no result collected?
        else:
            res_list = Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext2_main)(cur_date, cur_stock, div, trans_folder) for cur_stock in stock_list)
            for cur_res in res_list:
                if cur_res is not None:
                    final_set.append(cur_res)
            final_set = pd.DataFrame().append(final_set)
            if len(final_set)>0:
                final_set.to_csv(out_file, index=False)

def process_sig_ext2_main2(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext2/
    """
    complete_tag = 0.6
    div = 16
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return

    pick_list = get_date_period(cur_date, lag, all_date_list)
    day_set = []
    bar_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = fe.read_feather(cur_file)
        day_set.append(res[['tdate','id','tyield']])
        
        cur_file = '%s/init.%s.csv'%(out_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = pd.read_csv(cur_file)
        res['tdate'] = cur_pick
        bar_set.append(res)
            
    day_set = pd.DataFrame().append(day_set)
    bar_set = pd.DataFrame().append(bar_set)
    res = pd.merge(day_set, bar_set, on=['tdate','id'])
    #剔除交易时间不够的股票
    r1 = res[res['pos']==1].groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    if len(res)==0:
        return
    
    r1 = res.groupby(['id','pos'],as_index=False)[['tag']].median()
    r1.columns = ['id','pos','split']
    res = pd.merge(res, r1, on=['id','pos'])
    res['dir'] = 'high'
    res.loc[res['tag']<=res['split'], 'dir'] = 'low'
    r1 = res.groupby(['id','pos','dir'],as_index=False)[['tyield']].sum()
    r1['tag'] = r1['dir']+'_'+(r1['pos']+100).astype(str).str[1:]+'_%s'%(div)
    r1 = r1.pivot_table(index='id',columns='tag',values='tyield')
    r1.fillna(0, inplace=True)
    r1['id'] = r1.index
    r1.to_csv(out_file, index=False)

def process_sig_ext2(begin_date, end_date, output_dir, job_num, lag):
    """
    date range:
        [begin_date - lag, end_date]
    input:
    output:
        $output_dir/eod.sig.ext2/
    """
    out_folder = '%s/eod.sig.ext2/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)

    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]

    logging.info('process sig ext2')
    if job_num==0:
        for cur_date in date_list:
            process_sig_ext2_main2(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext2_main2)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext3_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $feats_root/stock_basic_oneminute/
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext3/
    """

    complete_tag = 0.6
    split = 0.2
    bar_folder = '%s/stock_basic_oneminute/'%(cfg.param_path_src_minbar_file)
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return

    pick_list = get_date_period(cur_date, lag, all_date_list)
    day_set = []
    bar_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            logging.error(cur_file + " not exists")
            return
        res = fe.read_feather(cur_file)
        day_set.append(res[['tdate','id','adjfactor']])
        
        cur_file = '%s/stock_basic_oneminute.%s.csv'%(bar_folder, cur_pick)
        if not os.path.exists(cur_file):
            logging.error(cur_file + " not exists")
            return
        res = pd.read_csv(cur_file)
        res["tdate"]=int(cur_pick)
        bar_set.append(res)
    day_set = pd.DataFrame().append(day_set)
    bar_set = pd.DataFrame().append(bar_set)
    res = pd.merge(day_set, bar_set, on=['tdate','id'])
    #剔除交易时间不够的股票
    r1 = res.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    if len(res)==0:
        return
    
    #成交量复权
    res['volume'] = res['volume']/res['adjfactor']
    res['absr'] = abs(res['close']/res['pclose']-1)
    del res['tdate']
    del res['adjfactor']
    del res['min']
    del res['pclose']
    del res['open']
    del res['high']
    del res['low']
    del res['close']
    del res['dealcnt']
    
    res['s1'] = res['volume']
    res['rankV'] = res.groupby(['id'],as_index=False)[['volume']].rank(method='dense')
    res['rankR'] = res.groupby(['id'],as_index=False)[['absr']].rank(method='dense')
    res['s2'] = res['rankR']+res['rankV']
    res.loc[res['volume']<=0, 'volume'] = np.nan
    res['s3'] = res['absr']/np.log(res['volume'])
    res['s4'] = res['absr']/np.power(res['volume'], -0.5)
    res['s5'] = res['absr']/np.power(res['volume'], -0.25)
    res['s6'] = res['absr']/np.power(res['volume'], -0.1)
    res['s7'] = res['absr']/np.power(res['volume'], 0.1)
    res['s8'] = res['absr']/np.power(res['volume'], 0.25)
    res['s9'] = res['absr']/np.power(res['volume'], 0.5)
    res['s10'] = res['absr']/np.power(res['volume'], 0.7)
    res.fillna(0, inplace=True)

    vwap_all = res.groupby(['id'],as_index=False)[['volume','amount']].sum()
    vwap_all.columns = ['id','totalV','totalA']
    vwap_all['vwap'] = vwap_all['totalA']/vwap_all['totalV']
    res = pd.merge(res, vwap_all[['id','totalV']], on='id')
    res['vp'] = res['volume']/res['totalV']
    
    del res['absr']
    del res['rankV']
    del res['rankR']
    del res['totalV']
    
    final_set = []
    idx_list = np.arange(1,11)
    for cur_idx in idx_list:
        cur_sig = 's%s'%(cur_idx)
        res.sort_values(by=['id',cur_sig], ascending=[True,False], inplace=True)
        res['split'] = res.groupby(['id'],as_index=False)[['vp']].cumsum()
        temp_set = res[res['split']<=split]
        temp_set = temp_set.groupby(['id'],as_index=False)[['volume','amount']].sum()
        temp_set['vwapS'] = temp_set['amount']/temp_set['volume']
        temp_set['type'] = cur_sig
        final_set.append(temp_set[['id','type','vwapS']])
    final_set = pd.DataFrame().append(final_set)
    final_set = pd.merge(final_set, vwap_all[['id','vwap']], on='id')
    final_set['sig'] = final_set['vwapS']/final_set['vwap']
    final_set = final_set.pivot_table(index='id',columns='type',values='sig')
    final_set['id'] = final_set.index
    final_set.to_csv(out_file, index=False)

def process_sig_ext3(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext3/
    """
    out_folder = '%s/eod.sig.ext3/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 10
    logging.info('process sig ext3')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext3_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext3_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def cr_reg_resr2(x1_,x2_):
	x1_=np.array(x1_)
	x2_=np.array(x2_)
	d = x1_.shape[1]
	x1_mean = x1_.sum(1) / d
	x2_mean = x2_.sum(1) / d
	x1_error = np.transpose(np.transpose(x1_, (1, 0)) - x1_mean, (1, 0))
	x2_error = np.transpose(np.transpose(x2_, (1, 0)) - x2_mean, (1, 0))
	ssm1 = (x1_error * x2_error).sum(1)
	ssm2 = (x2_error ** 2).sum(1) + fm_zero
	ss_tot = (x1_error ** 2).sum(1) + fm_zero
	beta1 = ssm1 / ssm2
	beta0 = x1_mean - beta1 * x2_mean
	ss_res = np.transpose(np.transpose(x1_, (1, 0)) - beta0 - np.transpose(x2_, (1, 0)) * beta1, (1, 0))
	ss_res_new = np.transpose(np.transpose(x1_, (1, 0)) - np.transpose(x2_, (1, 0)) * beta1, (1, 0))
	r2 = 1 - (ss_res ** 2).sum(1) / ss_tot
	return beta0, beta1, ss_res, ss_res_new, r2

def calc_reg(res, res_index, reg_type):
	x1 = res[res['type']==reg_type].pivot_table(index='id',columns='tdate',values='ret')
	x1.fillna(0, inplace=True)
	x2 = res_index[res_index.index==reg_type].values
	x2 = np.tile(x2, x1.shape[0]).reshape(x1.shape[0],-1)
	res = cr_reg_resr2(x1, x2)
	return (x1.index.values,)+res
	
# =============================================================================
# 	y = x1.iloc[0].values # 因变量为第 2 列数据
# 	x = x2[0,:] # 自变量为第 3 列数据
# 	x = sm.add_constant(x) # 若模型中有截距，必须有这一步
# 	model = sm.OLS(y, x).fit() # 构建最小二乘模型并拟合
# 	res = model.resid
# =============================================================================

def calc_stat(resid1, resid2, lag):
	resid = (resid1 - resid2)*10000
	b = np.std(resid, axis=1)
	b[np.isnan(b)] = np.nan
	b[b==0] = np.nan
	stat = np.mean(resid, axis=1)/b*np.power(lag, 0.5)
	stat[np.isnan(stat)] = 0
	return stat

def calc_reg_normal(y, x):
	x = sm.add_constant(x)
	model = sm.OLS(y, x).fit()
	return model.resid

def process_sig_ext4_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $feats_root/stock_bar/stock_basic_sixtyminute
        $feats_root/index_bar/index_basic_sixtyminute
    output:
        $output_dir/eod.sig.ext4
    """
    index_code = 399303
    complete_tag = 0.6
    stock_min_bar="stock_basic_sixtyminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, stock_min_bar)
    index_min_bar="index_basic_sixtyminute"
    index_folder = '%s/%s/'%(cfg.param_path_src_index_minbar_file, index_min_bar)

    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, lag, all_date_list)
    bar_set = []
    index_set = []
    for cur_pick in pick_list:
        cur_file = '%s/%s.%s.csv'%(bar_folder, stock_min_bar, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = pd.read_csv(cur_file)
        res["tdate"]=int(cur_pick)
        res.rename(columns={"min":"bar"},inplace=True) 
        bar_set.append(res)
        
        cur_file = '%s/%s.%s.csv'%(index_folder, index_min_bar, cur_pick)
        if os.path.exists(cur_file):
            res = pd.read_csv(cur_file)
            res["tdate"]=int(cur_pick)
            res.rename(columns={"min":"bar"},inplace=True)
            index_set.append(res)

    bar_set = pd.DataFrame().append(bar_set)
    index_set = pd.DataFrame().append(index_set)
    if len(index_set)==0:
        index_set = pd.DataFrame(columns=['tdate','id','bar','pclose','open','high','low','close','amount','volume'])
    index_set = index_set[index_set['id']==index_code]
    #剔除交易时间不够的股票
    r1 = bar_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    bar_set = bar_set[bar_set['id'].isin(r1['id'])]
    if len(bar_set)==0:
        return
    
    bar_set['lnret'] = np.log(bar_set['close']/bar_set['pclose'])
    bar_set['lnret'] = bar_set['lnret'].fillna(0)
    bar_set['type'] = ''
    bar_set.loc[bar_set['bar']==930, 'type'] = 'overnight'
    bar_set.loc[bar_set['bar']>930, 'type'] = 'am'
    bar_set.loc[bar_set['bar']>=1300, 'type'] = 'pm'
    
    index_set['lnret'] = np.log(index_set['close']/index_set['pclose'])
    #可能会存在价格为空的情况
    index_set.dropna(subset=['lnret'], inplace=True)
    index_set['type'] = ''
    index_set.loc[index_set['bar']==930, 'type'] = 'overnight'
    index_set.loc[index_set['bar']>930, 'type'] = 'am'
    index_set.loc[index_set['bar']>=1300, 'type'] = 'pm'
    
    res = bar_set.groupby(['id','tdate','type'],as_index=False)[['lnret']].sum()
    res['ret'] = np.power(np.exp(1), res['lnret'])-1
    
    if len(index_set)>0:
        res_index = index_set.groupby(['tdate','type'],as_index=False)[['lnret']].sum()
        res_index['bmk_ret'] = np.power(np.exp(1), res_index['lnret'])-1
        res_index['tag'] = 'ori'
    else:
        res_index = pd.DataFrame(columns=['tdate','type','bmk_ret','tag'])

    temp_set = res.groupby(['tdate','type'],as_index=False)[['ret']].mean()
    temp_set['bmk_ret'] = temp_set['ret']
    temp_set['tag'] = 'calc'
    res_index = res_index[['tdate','type','bmk_ret','tag']].append(temp_set[['tdate','type','bmk_ret','tag']])
    res_index.drop_duplicates(subset=['tdate','type'], keep='first', inplace=True)
    res_index = res_index.pivot_table(index='type',columns='tdate',values='bmk_ret')
    
    #APM因子方法论与W式切割方法的结合
    r1 = res.groupby(['id','type'],as_index=False)[['ret']].sum()
    r1 = r1.pivot_table(index='id',columns='type',values='ret')
    r1.fillna(0, inplace=True)
    r1['ovp'] = r1['overnight']-r1['pm']
    r1['avp'] = r1['am']-r1['pm']
    r1.reset_index(inplace=True)
    
    #过去n日收益率
    period_ret = bar_set.groupby(['id'],as_index=False)[['lnret']].sum()
    period_ret['ret'] = np.power(np.exp(1), period_ret['lnret'])-1

    if not reproduce:
        debug_output("eod.sig.ext4", cur_date, "period_ret.before_calc_reg", period_ret)
        debug_output("eod.sig.ext4", cur_date, "res.before_calc_reg", res)
        debug_output("eod.sig.ext4", cur_date, "res_index.before_calc_reg", res_index)
    else:
        period_ret = debug_load("eod.sig.ext4", cur_date, "period_ret.before_calc_reg")
        res = debug_load("eod.sig.ext4", cur_date, "res.before_calc_reg") 
        res_index = debug_load("eod.sig.ext4", cur_date, "res_index.before_calc_reg")

    #第一阶段回归残差
    id_list, beta0, beta1, resid1, resid1_new, rsquared = calc_reg(res, res_index, 'overnight')
    #第二阶段回归残差
    id_list, beta0, beta1, resid2, resid2_new, rsquared = calc_reg(res, res_index, 'pm')
    s1 = calc_stat(resid1, resid2, lag)
    s2 = calc_stat(resid1_new, resid2_new, lag)
    stat = pd.DataFrame({'id':id_list, 'stat':s1, 'stat2':s2})
    stat = pd.merge(period_ret, stat, on='id')
    
    stat['apm_improve'] = calc_reg_normal(stat['stat'].values, stat['ret'].values)
    stat['apm_improve2'] = calc_reg_normal(stat['stat2'].values, stat['ret'].values)

    stat = pd.merge(stat, r1, on='id')
    stat[['id','apm_improve','apm_improve2','ovp','avp']].to_csv(out_file, index=False)

def __ext4_func(res, res_index, period_ret):
    lag=20
    #第一阶段回归残差
    id_list, beta0, beta1, resid1, resid1_new, rsquared = calc_reg(res, res_index, 'overnight')
    #第二阶段回归残差
    id_list, beta0, beta1, resid2, resid2_new, rsquared = calc_reg(res, res_index, 'pm')

    s1 = calc_stat(resid1, resid2, lag)
    s2 = calc_stat(resid1_new, resid2_new, lag)
    stat = pd.DataFrame({'id':id_list, 'stat':s1, 'stat2':s2})
    stat = pd.merge(period_ret, stat, on='id')

    stat['apm_improve'] = calc_reg_normal(stat['stat'].values, stat['ret'].values)
    stat['apm_improve2'] = calc_reg_normal(stat['stat2'].values, stat['ret'].values)

    #stat = pd.merge(stat, r1, on='id')
    result = stat[['id','apm_improve','apm_improve2']]
    return result

def __ext4_func_validate(result):
    target_data = pd.read_csv("/hdd5/emily/debug_out_qy/eod.sig.ext4/20230307/result.csv")   
    corr1=result["apm_improve"].corr(target_data["apm_improve"])
    corr2=result["apm_improve2"].corr(target_data["apm_improve2"])
    print("apm_improve corr:{}, apm_improve2 corr:{}".format(corr1, corr2))
    return (corr1 > 0.9 and corr2 > 0.99)

def __debug_ext4_res_diff(res, target_res):
    res=res.set_index(["id","tdate","type"])
    target_res=target_res.set_index(["id","tdate","type"])

    tickers = res.index.get_level_values(0).unique()
    diff_tickers = []
    diff_corr = []
    for ticker in tickers:
        ret_corr=res.loc[ticker]["ret"].corr(target_res.loc[ticker]["ret"])
        if ret_corr < 0.99:
            diff_tickers.append(ticker)
            diff_corr.append(ret_corr)
    return pd.DataFrame({"corr":diff_corr}, index=diff_tickers).sort_values("corr")


def __debug_ext4_3():

    cur_date = 20230307
    cfg.debug_output_dir="/hdd5/emily/debug_out/"
    period_ret = debug_load("eod.sig.ext4", cur_date, "period_ret.before_calc_reg")
    res = debug_load("eod.sig.ext4", cur_date, "res.before_calc_reg")
    res_index = debug_load("eod.sig.ext4", cur_date, "res_index.before_calc_reg")

    cfg.debug_output_dir="/hdd5/emily/debug_out_qy/"
    target_res = debug_load("eod.sig.ext4", cur_date, "res.before_calc_reg")

    res.set_index(["id","tdate","type"], inplace=True)
    target_res.set_index(["id","tdate","type"], inplace=True)

    tol = 0
    gap = (abs(res["lnret"]-target_res["lnret"])>tol)
    print("gap percentage={}".format(len(res[gap])/len(res)))
    res[gap] = target_res[gap]
   
    res.reset_index(inplace=True)
    target_res.reset_index(inplace=True)

    
    __ext4_func_validate(__ext4_func(res, res_index, period_ret))

    #diff_data=__debug_ext4_res_diff(res, target_res)
    
#    for top_ticker in diff_data.index:
#        res[res["id"]==top_ticker] = target_res[target_res["id"]==top_ticker]
#        print("try {} corr_diff={}".format(top_ticker, diff_data.loc[top_ticker]))
#        if __ext4_func_validate(__ext4_func(res, res_index, period_ret)):
#            print("fix succeed!")
#            break
        # recover res
        # cfg.debug_output_dir="/hdd5/emily/debug_out/"
        # res = debug_load("eod.sig.ext4", cur_date, "res.before_calc_reg")


def __debug_ext4_2():
    cur_date = 20230307
    cfg.debug_output_dir="/hdd5/emily/debug_out_qy/"
    period_ret1 = debug_load("eod.sig.ext4", cur_date, "period_ret.before_calc_reg")
    res1 = debug_load("eod.sig.ext4", cur_date, "res.before_calc_reg")
    res_index1 = debug_load("eod.sig.ext4", cur_date, "res_index.before_calc_reg")
    res_index1.index=pd.Index(['am', 'overnight', 'pm'], dtype='object', name='type')
    res_index1.columns = pd.Int64Index([20230208, 20230209, 20230210, 20230213, 20230214, 20230215,
            20230216, 20230217, 20230220, 20230221, 20230222, 20230223,
            20230224, 20230227, 20230228, 20230301, 20230302, 20230303,
            20230306, 20230307],
           dtype='int64', name='tdate')

    resid1_1 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid1"), allow_pickle=True)
    resid2_1 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid2"), allow_pickle=True)
    resid1_new_1 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid1_new"), allow_pickle=True)
    resid2_new_1 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid2_new"), allow_pickle=True)
    s1_1 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "s1"), allow_pickle=True)
    s2_1 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "s2"), allow_pickle=True)

    cfg.debug_output_dir="/hdd5/emily/debug_out/"
    period_ret2 = debug_load("eod.sig.ext4", cur_date, "period_ret.before_calc_reg")
    res2 = debug_load("eod.sig.ext4", cur_date, "res.before_calc_reg")
    res_index2 = debug_load("eod.sig.ext4", cur_date, "res_index.before_calc_reg")

    resid1_2 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid1"), allow_pickle=True)
    resid2_2 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid2"), allow_pickle=True)
    resid1_new_2 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid1_new"), allow_pickle=True)
    resid2_new_2 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid2_new"), allow_pickle=True)
    s1_2 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "s1"), allow_pickle=True)
    s2_2 = np.load(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "s2"), allow_pickle=True)

    print("period_ret lnret:{}".format(period_ret1["lnret"].corr(period_ret2["lnret"])))
    print("period_ret ret:{}".format(period_ret1["ret"].corr(period_ret2["ret"])))

    res1.set_index(["id","tdate","type"],inplace=True)
    res2.set_index(["id","tdate","type"],inplace=True)
    res=res1.join(res2, lsuffix="_1", rsuffix="_2")
    print("res lnret:{}".format(res["lnret_1"].corr(res["lnret_2"])))
    print("res ret:{}".format(res["ret_1"].corr(res["ret_2"])))
   

    tickers = res.index.get_level_values(0).unique()
    for ticker in tickers:
        ret_corr=res1.loc[ticker]["ret"].corr(res2.loc[ticker]["ret"])
        if ret_corr < 0.99:
            print("res {} {}".format(ticker, ret_corr))

    import pdb
    pdb.set_trace()


    print("res_index, am:{}".format(res_index1.loc["am"].corr(res_index2.loc["am"])))
    print("res_index, overnight:{}".format(res_index1.loc["overnight"].corr(res_index2.loc["overnight"])))
    print("res_index, pm:{}".format(res_index1.loc["pm"].corr(res_index2.loc["pm"])))

    def comp_np_arr(name, arr1, arr2):
        if len(arr1.shape) == 2:
            m1, n1 = arr1.shape
        else:
            m1 = arr1.shape
            n1 = 1
        if len(arr2.shape) == 2:
            m2, n2 = arr2.shape
        else:
            m2 = arr2.shape
            n2 = 1

        assert m1 == m2 and n1 == n2

        gap = abs((arr2 - arr1)/arr1)
        if n1 > 1:
            vals = gap.reshape(m1*n1, 1)
        else:
            vals = gap
        pbars=[10,50,80,90,99,99.5,99.9]
        pvals = np.percentile(vals, pbars)
        data=pd.DataFrame({"values":pvals}, index=pbars)
        print("{} gap:\n{}".format(name,data))

    comp_np_arr("resid1", resid1_1, resid1_2)
    comp_np_arr("resid2", resid2_1, resid2_2)
    comp_np_arr("resid1_new", resid1_new_1, resid1_new_2)
    comp_np_arr("resid2_new", resid2_new_1, resid2_new_2)
    comp_np_arr("s1", s1_1, s1_2)
    comp_np_arr("s2", s2_1, s2_2)

def __debug_ext4():
    cur_date = 20230307
    lag = 20
    period_ret = debug_load("eod.sig.ext4", cur_date, "period_ret.before_calc_reg")
    res_index = debug_load("eod.sig.ext4", cur_date, "res_index.before_calc_reg")
    res = debug_load("eod.sig.ext4", cur_date, "res.before_calc_reg") 

    res_index.index=pd.Index(['am', 'overnight', 'pm'], dtype='object', name='type')
    res_index.columns = pd.Int64Index([20230208, 20230209, 20230210, 20230213, 20230214, 20230215,
            20230216, 20230217, 20230220, 20230221, 20230222, 20230223,
            20230224, 20230227, 20230228, 20230301, 20230302, 20230303,
            20230306, 20230307],
           dtype='int64', name='tdate')

    #第一阶段回归残差
    id_list, beta0, beta1, resid1, resid1_new, rsquared = calc_reg(res, res_index, 'overnight')
    #第二阶段回归残差
    id_list, beta0, beta1, resid2, resid2_new, rsquared = calc_reg(res, res_index, 'pm')

    resid1.dump(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid1"))
    resid2.dump(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid2"))
    resid1_new.dump(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid1_new"))
    resid2_new.dump(os.path.join(cfg.debug_output_dir, "eod.sig.ext4", "{}".format(cur_date), "resid2_new"))

    s1 = calc_stat(resid1, resid2, lag)
    s2 = calc_stat(resid1_new, resid2_new, lag)
    stat = pd.DataFrame({'id':id_list, 'stat':s1, 'stat2':s2})
    stat = pd.merge(period_ret, stat, on='id')

    s1.dump(os.path.join(cfg.debug_output_dir, "eod.sig.ext4","{}".format(cur_date), "s1"))
    s2.dump(os.path.join(cfg.debug_output_dir, "eod.sig.ext4","{}".format(cur_date), "s2"))
    
    stat['apm_improve'] = calc_reg_normal(stat['stat'].values, stat['ret'].values)
    stat['apm_improve2'] = calc_reg_normal(stat['stat2'].values, stat['ret'].values)

    #stat = pd.merge(stat, r1, on='id')
    result = stat[['id','apm_improve','apm_improve2']]

    outfile = "/hdd5/emily/debug_out/eod.sig.ext4/{}/result.csv".format(cur_date)
    #outfile = "/hdd5/emily/debug_out_qy/eod.sig.ext4/{}/result.csv".format(cur_date)
    result.to_csv(outfile, index=False)

def process_sig_ext4(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext4
    """

    out_folder = '%s/eod.sig.ext4/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext4')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext4_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext4_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext5_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        database wind:ASHAREMONEYFLOW 
    output:
        $output_dir/eod.sig.ext5/
    """
    
    col_list = ['b_exl','s_exl','b_l','s_l','b_m','s_m','b_s','s_s','a_exl','a_l','a_m','a_s']
    complete_tag = 0.6
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, lag, all_date_list)
    res = get_moneyflow(min(pick_list), max(pick_list))
    r1 = res[res['tdate']==cur_date]
    if len(r1)==0:
        return
    r1 = res.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    if len(res)==0:
        return
    
    r1 = res.groupby(['id'],as_index=False)[col_list+['amount']].sum()
    for cur_col in col_list:
        r1[cur_col] = r1[cur_col]/r1['amount']
    
    r1['bevl'] = calc_reg_normal(r1['b_exl'].values, r1['b_l'].values)
    r1['sevl'] = calc_reg_normal(r1['s_exl'].values, r1['s_l'].values)
    r1['aevl'] = calc_reg_normal(r1['a_exl'].values, r1['a_l'].values)
    r1['bevm'] = calc_reg_normal(r1['b_exl'].values, r1['b_m'].values)
    r1['sevm'] = calc_reg_normal(r1['s_exl'].values, r1['s_m'].values)
    r1['aevm'] = calc_reg_normal(r1['a_exl'].values, r1['a_m'].values)
    r1['bevs'] = calc_reg_normal(r1['b_exl'].values, r1['b_s'].values)
    r1['sevs'] = calc_reg_normal(r1['s_exl'].values, r1['s_s'].values)
    r1['aevs'] = calc_reg_normal(r1['a_exl'].values, r1['a_s'].values)
    r1[['id','bevl', 'sevl', 'aevl', 'bevm','sevm', 'aevm', 'bevs', 'sevs', 'aevs']].to_csv(out_file, index=False)

def get_moneyflow(begin_date, end_date):
    col_list = ['b_exl','s_exl','b_l','s_l','b_m','s_m','b_s','s_s']
    sql = "select TRADE_DT, S_INFO_WINDCODE, \
                    BUY_VALUE_EXLARGE_ORDER, SELL_VALUE_EXLARGE_ORDER, \
                    BUY_VALUE_LARGE_ORDER, SELL_VALUE_LARGE_ORDER, \
                    BUY_VALUE_MED_ORDER, SELL_VALUE_MED_ORDER, \
                    BUY_VALUE_SMALL_ORDER, SELL_VALUE_SMALL_ORDER \
                    from ASHAREMONEYFLOW  \
                    where TRADE_DT between '%s' and '%s'" % (begin_date, end_date)
    res = util_db.get_result(util_db.dbsource_wind, sql)
    res = pd.DataFrame(res, columns=['tdate','id']+col_list)
    res['id'] = res['id'].apply(lambda x :util_other.id_to_int(x))
    res = res.loc[res.id!=0,:]
    res[['id','tdate']] = res[['id','tdate']].astype(int)
    res[col_list] = res[col_list].astype(float)
    res.fillna(0, inplace=True)
    res['amount'] = res[col_list].sum(axis=1)
    res['a_exl'] = res['b_exl']+res['s_exl']
    res['a_l'] = res['b_l']+res['s_l']
    res['a_m'] = res['b_m']+res['s_m']
    res['a_s'] = res['b_s']+res['s_s']
    return res

def process_sig_ext5(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext5/
    """
    out_folder = '%s/eod.sig.ext5/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext5')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext5_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext5_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext6_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext6/
    """
    complete_tag = 0.6
    split_list = [0.2,0.25,0.3]
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, lag, all_date_list)
    day_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = fe.read_feather(cur_file)
        day_set.append(res)

    day_set = pd.DataFrame().append(day_set)
    day_set = day_set[day_set['volume']>0]
    day_set = day_set[~((day_set['open']==day_set['high']) & (day_set['high']==day_set['low']) & (day_set['low']==day_set['close']))]
    r1 = day_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    day_set = day_set[day_set['id'].isin(r1['id'])]
    if len(day_set)==0:
        return
    
    day_set['amplitude'] = day_set['high']/day_set['low']-1
    day_set['adjclose'] = day_set['close']*day_set['adjfactor']
    final_set = None
    for cur_split in split_list:
        res = day_set.groupby(['id'],as_index=False)[['adjclose']].quantile([cur_split, 1-cur_split])
        res.reset_index(inplace=True)
        res.loc[res['level_1']==cur_split, 'level_1'] = 'v_low'
        res.loc[res['level_1']==(1-cur_split), 'level_1'] = 'v_high'
        res = res.pivot_table(index='id',columns='level_1',values='adjclose')
        res.reset_index(inplace=True)
        temp_set = pd.merge(day_set, res, on='id')
        v_high = temp_set[temp_set['adjclose']>=temp_set['v_high']].groupby(['id'],as_index=False)[['amplitude','to']].mean()
        v_low = temp_set[temp_set['adjclose']<=temp_set['v_low']].groupby(['id'],as_index=False)[['amplitude','to']].mean()
        v_high.columns = ['id','high','high_to']
        v_low.columns = ['id','low','low_to']
        temp_set = pd.merge(v_high, v_low, on='id')
        temp_set['amplitude_%s'%(int(cur_split*100))] = temp_set['high']-temp_set['low']
        temp_set['turnover_%s'%(int(cur_split*100))] = temp_set['high_to']-temp_set['low_to']
        del temp_set['high']
        del temp_set['low']
        del temp_set['high_to']
        del temp_set['low_to']
        if final_set is None:
            final_set = temp_set
        else:
            final_set = pd.merge(final_set, temp_set, on='id')
    final_set.to_csv(out_file, index=False)

def process_sig_ext6(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext6/
    """
    out_folder = '%s/eod.sig.ext6/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(19000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext6')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext6_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext6_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def get_moneyflow2(begin_date, end_date):
	col_list = ['b_exl','s_exl','b_l','s_l','b_m','s_m','b_s','s_s']
	sql = "select TRADE_DT, S_INFO_WINDCODE, \
			BUY_VALUE_EXLARGE_ORDER_ACT,SELL_VALUE_EXLARGE_ORDER_ACT, \
			BUY_VALUE_LARGE_ORDER_ACT,SELL_VALUE_LARGE_ORDER_ACT, \
			BUY_VALUE_MED_ORDER_ACT,SELL_VALUE_MED_ORDER_ACT, \
			BUY_VALUE_SMALL_ORDER_ACT,SELL_VALUE_SMALL_ORDER_ACT \
			from ASHAREMONEYFLOW \
			where TRADE_DT between '%s' and '%s'" % (begin_date, end_date)
	res = util_db.get_result(util_db.dbsource_wind, sql)
	res = pd.DataFrame(res, columns=['tdate','id']+col_list)
	res['id'] = res['id'].apply(lambda x :util_other.id_to_int(x))
	res = res.loc[res.id!=0,:]
	res[['id','tdate']] = res[['id','tdate']].astype(int)
	res[col_list] = res[col_list].astype(float)
	res.fillna(0, inplace=True)
	return res

def process_sig_ext7_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext7/ 
    """

    complete_tag = 0.6
    split_list = [0.1,0.2,0.3]
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, lag, all_date_list)
    day_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = fe.read_feather(cur_file)
        day_set.append(res)
    day_set = pd.DataFrame().append(day_set)
    #剔除停牌
    day_set = day_set[day_set['volume']>0]
    #剔除一字涨跌停
    day_set = day_set[~((day_set['open']==day_set['high']) & (day_set['high']==day_set['low']) & (day_set['low']==day_set['close']))]
    #剔除交易时间过少
    r1 = day_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    day_set = day_set[day_set['id'].isin(r1['id'])]
    
    res = get_moneyflow2(min(pick_list), max(pick_list))
    r1 = res.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    day_set = pd.merge(res, day_set[['id','tdate','tyield']], on=['id','tdate'])
    if len(day_set)==0:
        return
    
    b = day_set['b_l']+day_set['b_m']+day_set['s_l']+day_set['s_m']
    b[b==0] = np.nan
    day_set['act_pos'] = (day_set['b_l']+day_set['b_m']-day_set['s_l']-day_set['s_m'])/b
    b = day_set['b_s']+day_set['s_s']
    b[b==0] = np.nan
    day_set['act_neg'] = (day_set['b_s']-day_set['s_s'])/b
    day_set.fillna(0, inplace=True)
    
    final_set = None
    for cur_split in split_list:
        res = day_set.groupby(['id'],as_index=False)[['tyield']].quantile([cur_split, 1-cur_split])
        res.reset_index(inplace=True)
        res.loc[res['level_1']==cur_split, 'level_1'] = 'v_low'
        res.loc[res['level_1']==(1-cur_split), 'level_1'] = 'v_high'
        res = res.pivot_table(index='id',columns='level_1',values='tyield')
        res.reset_index(inplace=True)
        temp_set = pd.merge(day_set, res, on='id')
        v_high = temp_set[temp_set['tyield']>=temp_set['v_high']].groupby(['id'],as_index=False)[['act_pos']].mean()
        v_low = temp_set[temp_set['tyield']<=temp_set['v_low']].groupby(['id'],as_index=False)[['act_neg']].mean()
        v_high.columns = ['id','high']
        v_low.columns = ['id','low']
        temp_set = pd.merge(v_high, v_low, on='id')
        temp_set['act_%s'%(int(cur_split*100))] = temp_set['high']-temp_set['low']
        temp_set.rename(columns={'high':'act_pos_%s'%(int(cur_split*100)), 'low':'act_neg_%s'%(int(cur_split*100))}, inplace = True)
        if final_set is None:
            final_set = temp_set
        else:
            final_set = pd.merge(final_set, temp_set, on='id')
    final_set.to_csv(out_file, index=False)

def process_sig_ext7(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext7/ 
    """
    out_folder = '%s/eod.sig.ext7/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(19000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext7')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext7_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext7_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext8_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext8
    """

    complete_tag = 0.6
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, lag, all_date_list)
    day_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = fe.read_feather(cur_file)
        day_set.append(res)
    day_set = pd.DataFrame().append(day_set)
    #剔除停牌
    day_set = day_set[day_set['volume']>0]
    #剔除一字涨跌停
    day_set = day_set[~((day_set['open']==day_set['high']) & (day_set['high']==day_set['low']) & (day_set['low']==day_set['close']))]
    #剔除交易时间过少
    r1 = day_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    day_set = day_set[day_set['id'].isin(r1['id'])]
    
    res = get_moneyflow(min(pick_list), max(pick_list))
    del res['amount']
    r1 = res.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    day_set = pd.merge(res, day_set[['id','tdate','amount','tyield']], on=['id','tdate'])
    if len(day_set)==0:
        return
    
    day_set['amount'] = day_set['amount']/10000
    day_set['lnret'] = np.log(day_set['tyield']+1)
    day_set['diff_l'] = day_set['b_l']-day_set['s_l']
    day_set['abs_diff_l'] = abs(day_set['diff_l'])
    day_set['sum_l'] = day_set['b_l']+day_set['s_l']
    day_set['diff_s'] = day_set['b_s']-day_set['s_s']
    day_set['abs_diff_s'] = abs(day_set['diff_s'])
    day_set['sum_s'] = day_set['b_s']+day_set['s_s']
    
    res = day_set.groupby(['id'],as_index=False)[['amount','diff_l','abs_diff_l','sum_l','diff_s','abs_diff_s','sum_s','lnret']].sum()
    res.loc[res['amount']==0,'amount'] = np.nan
    res.loc[res['sum_l']==0,'sum_l'] = np.nan
    res.loc[res['sum_s']==0,'sum_s'] = np.nan
    res.loc[res['abs_diff_l']==0,'abs_diff_l'] = np.nan
    res.loc[res['abs_diff_s']==0,'abs_diff_s'] = np.nan
    res['s1_l'] = res['diff_l']/res['amount']
    res['s2_l'] = res['diff_l']/res['sum_l']
    res['s3_l'] = res['diff_l']/res['abs_diff_l']
    res['s1_s'] = res['diff_s']/res['amount']
    res['s2_s'] = res['diff_s']/res['sum_s']
    res['s3_s'] = res['diff_s']/res['abs_diff_s']
    res.fillna(0,inplace=True)
    res['ret'] = np.power(np.exp(1), res['lnret'])-1
    res['st_l'] = calc_reg_normal(res['s3_l'].values, res['ret'].values)
    res['st_s'] = calc_reg_normal(res['s3_s'].values, res['ret'].values)
    res['ret_l'] = calc_reg_normal(res['ret'].values, res['s3_l'].values)
    res['ret_s'] = calc_reg_normal(res['ret'].values, res['s3_s'].values)
    res = res[['id','s1_l', 's2_l', 's3_l', 's1_s', 's2_s', 's3_s','st_l', 'st_s', 'ret_l', 'ret_s']]
    res.to_csv(out_file, index=False)

def process_sig_ext8(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext8
    """
    out_folder = '%s/eod.sig.ext8/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(19000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext8')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext8_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext8_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext9_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext9/
    """
    lag = lag+1
    complete_tag = 0.6
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return

    pick_list = get_date_period(cur_date, lag, all_date_list)
    day_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = fe.read_feather(cur_file)
        day_set.append(res)
    day_set = pd.DataFrame().append(day_set)
    #剔除停牌
    day_set = day_set[day_set['volume']>0]
    #剔除一字涨跌停
    day_set = day_set[~((day_set['open']==day_set['high']) & (day_set['high']==day_set['low']) & (day_set['low']==day_set['close']))]
    #剔除交易时间过少
    r1 = day_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    day_set = day_set[day_set['id'].isin(r1['id'])]

    res = get_moneyflow(min(pick_list), max(pick_list))
    r1 = res.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    day_set = pd.merge(res, day_set[['id','tdate','tyield']], on=['id','tdate'])
    if len(day_set)==0:
        return
    
    #净流入
    day_set['in_exl'] = day_set['b_exl']-day_set['s_exl']
    day_set['in_l'] = day_set['b_l']-day_set['s_l']
    day_set['in_m'] = day_set['b_m']-day_set['s_m']
    day_set['in_s'] = day_set['b_s']-day_set['s_s']
    day_set.sort_values(by=['id','tdate'], inplace=True)
    
    #同步相关性
    r1 = day_set[day_set['tdate']>=pick_list[1]].copy()
    r2 = r1.groupby(['id'],as_index=False)[['in_exl','in_l','in_m','in_s']].rank()
    r2['id'] = r1['id']
    r1 = r2.groupby(['id'])[['in_exl','in_l','in_m','in_s']].corr()
    r1.reset_index(inplace=True)
    r1 = pd.melt(r1, id_vars=['id','level_1'])
    r1['tag'] = r1['level_1']+'#'+r1['variable']
    r1 = r1[r1['tag'].isin(['in_exl#in_l','in_exl#in_m','in_exl#in_s','in_l#in_m','in_l#in_s','in_m#in_s'])]
    r1 = r1.pivot_table(index='id',columns='tag',values='value')
    r1.reset_index(inplace=True)
    r1.rename(columns={'in_exl#in_l':'cor_exl_l', 'in_exl#in_m':'cor_exl_m', 'in_exl#in_s':'cor_exl_s',
                                    'in_l#in_m':'cor_l_m', 'in_l#in_s':'cor_l_s', 'in_m#in_s':'cor_m_s'}, inplace = True)
    feat1 = r1.copy()
    
    #错位相关性
    day_set['rank_date'] = day_set.groupby(['id'],as_index=False)[['tdate']].rank()
    r1 = day_set.groupby(['id'],as_index=False)[['rank_date']].max()
    r1.columns = ['id','max']
    r1['max'] = r1['max']-1
    day_set = pd.merge(day_set, r1, on=['id'])
    #t+0
    r1 = day_set[day_set['rank_date']<=day_set['max']]
    r2 = r1.groupby(['id'],as_index=False)[['in_exl','in_l','in_m','in_s','tyield']].rank()
    r2['id'] = r1['id']
    r2.reset_index(drop=True, inplace=True)
    #t+1
    r1 = day_set[day_set['rank_date']>1]
    r3 = r1.groupby(['id'],as_index=False)[['in_exl','in_l','in_m','in_s','tyield']].rank()
    r3['id'] = r1['id']
    r3.columns = ['in_exl1', 'in_l1', 'in_m1', 'in_s1', 'tyield1', 'id']
    r3.reset_index(drop=True, inplace=True)
    del r3['id']
    r2 = pd.concat([r2,r3], axis=1)
    r1 = r2.groupby(['id'])[['in_exl', 'in_l', 'in_m', 'in_s', 'tyield', 'in_exl1', 'in_l1','in_m1', 'in_s1']].corr()
    r1.reset_index(inplace=True)
    r1 = pd.melt(r1, id_vars=['id','level_1'])
    r1['tag'] = r1['level_1']+'#'+r1['variable']
    r1 = r1[r1['tag'].isin(['in_exl#in_exl1','in_exl#in_l1','in_exl#in_m1','in_exl#in_s1',
                                                     'in_l#in_exl1','in_l#in_l1','in_l#in_m1','in_l#in_s1',
                                                     'in_m#in_exl1','in_m#in_l1','in_m#in_m1','in_m#in_s1',
                                                     'in_s#in_exl1','in_s#in_l1','in_s#in_m1','in_s#in_s1','tyield#in_s1'])]
    r1 = r1.pivot_table(index='id',columns='tag',values='value')
    r1.reset_index(inplace=True)
    r1.rename(columns={'in_exl#in_exl1':'cor_exl_exl1',
                                            'in_exl#in_l1':'cor_exl_l1',
                                            'in_exl#in_m1':'cor_exl_m1',
                                            'in_exl#in_s1':'cor_exl_s1',
                                            'in_l#in_exl1':'cor_l_exl1',
                                            'in_l#in_l1':'cor_l_l1',
                                            'in_l#in_m1':'cor_l_m1',
                                            'in_l#in_s1':'cor_l_s1',
                                            'in_m#in_exl1':'cor_m_exl1',
                                            'in_m#in_l1':'cor_m_l1',
                                            'in_m#in_m1':'cor_m_m1',
                                            'in_m#in_s1':'cor_m_s1',
                                            'in_s#in_exl1':'cor_s_exl1',
                                            'in_s#in_l1':'cor_s_l1',
                                            'in_s#in_m1':'cor_s_m1',
                                            'in_s#in_s1':'cor_s_s1',
                                            'tyield#in_s1':'cor_r_s1'}, inplace = True)
    
    res = pd.merge(feat1, r1, on=['id'], how='outer')
    res.to_csv(out_file, index=False)

def process_sig_ext9(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext9/
    """
    out_folder = '%s/eod.sig.ext9/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(19000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext9')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext9_main(cur_date, lag, all_date_list, out_folder)
    else:
        #进程太多会导致sqlserver异常
        Parallel(n_jobs=10, backend="loky", verbose=param_verbose)(delayed(process_sig_ext9_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def calc_sr(cur_date, lag, complete_tag, all_date_list, bar_folder, bar_name):
    split = 0.2
    blank_res = pd.DataFrame(columns=['id','sr'])
    pick_list = get_date_period(cur_date, lag, all_date_list)
    bar_set = []
    for cur_pick in pick_list:
        cur_file = '%s/%s.%s.csv'%(bar_folder, bar_name, cur_pick)
        if not os.path.exists(cur_file):
            return blank_res
        res = pd.read_csv(cur_file)
        res["tdate"]=int(cur_pick)
        res.rename(columns={"min":"bar", "dealcnt":"trdcount"}, inplace=True)
        bar_set.append(res[['tdate','id','amount','trdcount','close','pclose']])
    bar_set = pd.DataFrame().append(bar_set)
    bar_set = bar_set[bar_set['trdcount']>0]
    bar_set['avg_amt'] = bar_set['amount']/bar_set['trdcount']
    bar_set['lnret'] = np.log(bar_set['close']/bar_set['pclose'])
    #剔除交易时间不够的股票
    r1 = bar_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    bar_set = bar_set[bar_set['id'].isin(r1['id'])]
    if len(bar_set)==0:
        return blank_res
    
    res = bar_set.groupby(['id'],as_index=False)[['avg_amt']].sum()
    res.columns = ['id','total_amt']
    res = pd.merge(bar_set, res, on='id')
    res['ap'] = res['avg_amt']/res['total_amt']
    res.sort_values(by=['id','avg_amt'], ascending=[True,False], inplace=True)
    res['split'] = res.groupby(['id'],as_index=False)[['ap']].cumsum()
    temp_set = res[res['split']<=split]
    temp_set = temp_set.groupby(['id'],as_index=False)[['lnret']].sum()
    temp_set['sr'] = np.power(np.exp(1), temp_set['lnret'])-1
    return temp_set[['id','sr']]

def calc_amt(cur_date,min_bar,del_rank,bar_folder,bar_name,daily_folder):
    feat0 = pd.DataFrame(columns=['id','amt_std','amt_cv'])
    feat1 = pd.DataFrame(columns=['id','QUA'])
    feat2 = pd.DataFrame(columns=['id','amt_skew','amt_kurt'])
    feat3 = pd.DataFrame(columns=['id','ts','te'])
    cur_file = '%s/%s.%s.csv'%(bar_folder, bar_name, cur_date)
    if not os.path.exists(cur_file):
        return feat0,feat1,feat2,feat3
    
    bar_set = pd.read_csv(cur_file)
    bar_set.rename(columns={"min":"bar", "dealcnt":"trdcount"}, inplace=True)

    bar_set = bar_set[bar_set['trdcount']>0]
    bar_set['avg_amt'] = bar_set['amount']/bar_set['trdcount']
    
    cur_file = '%s/basic.%s.feather'%(daily_folder, cur_date)
    if not os.path.exists(cur_file):
        return feat0,feat1,feat2,feat3
    day_set = fe.read_feather(cur_file)
    day_set = day_set[day_set['high']!=day_set['low']]
    bar_set = bar_set[bar_set['id'].isin(day_set['id'])]
    
    res = bar_set.groupby(['id'],as_index=False)[['bar']].count()
    res = res[res['bar']>=min_bar]
    bar_set = bar_set[bar_set['id'].isin(res['id'])]
    if len(bar_set)==0:
        return feat0,feat1,feat2,feat3
    
    #STD
    r1 = bar_set.groupby(['id'],as_index=False)[['avg_amt']].quantile([0.1])
    r1.reset_index(drop=True,inplace=True)
    r1.columns = ['id','q10']
    r1 = pd.merge(bar_set, r1, on='id')
    r1 = r1[r1['avg_amt']<=r1['q10']]
    feat0 = r1.groupby(['id'],as_index=False)[['avg_amt']].agg(['mean','std'])
    feat0.reset_index(inplace=True)
    feat0.columns = ['id','mean','amt_std']
    feat0['amt_cv'] = feat0['amt_std']/feat0['mean']
    feat0 = feat0[['id','amt_std','amt_cv']]
    
    #QUA
    bar_set['rank_amt'] = bar_set.groupby(['id'],as_index=False)[['avg_amt']].rank(method='dense', ascending=False)
    res = bar_set[bar_set['rank_amt']>del_rank]
    r1 = res.groupby(['id'],as_index=False)[['avg_amt']].agg(['min','max'])
    r1.reset_index(inplace=True)
    r1.columns = ['id','min','max']
    r2 = res.groupby(['id'],as_index=False)[['avg_amt']].quantile([0.1])
    r2.reset_index(drop=True,inplace=True)
    feat1 = pd.merge(r1, r2, on='id')
    feat1['QUA'] = (feat1['avg_amt']-feat1['min'])/(feat1['max']-feat1['min'])
    feat1 = feat1[['id','QUA']]
    
    #skew,kurt
    r1 = bar_set.groupby(['id'],as_index=False)[['avg_amt']].quantile([0.5])
    r1.reset_index(drop=True,inplace=True)
    r1.columns = ['id','q50']
    r1 = pd.merge(bar_set, r1, on='id')
    r1 = r1[r1['avg_amt']<=r1['q50']]
    r1 = r1.groupby(['id'],as_index=False)[['avg_amt']]
    r2 = r1.skew()
    r3 = r1.apply(lambda x:x.kurt())
    r2.columns = ['id','amt_skew']
    r3.columns = ['id','amt_kurt']
    feat2 = pd.merge(r2, r3, on='id')
    
    #ts,te
    r1 = res.groupby(['id'])[['avg_amt','amount','close']].corr()
    r1.reset_index(inplace=True)
    r1 = pd.melt(r1, id_vars=['id','level_1'])
    r1['tag'] = r1['level_1']+'#'+r1['variable']
    r1 = r1[r1['tag'].isin(['avg_amt#amount','avg_amt#close'])]
    r1 = r1.pivot_table(index='id',columns='tag',values='value')
    r1.reset_index(inplace=True)
    r1.rename(columns={'avg_amt#amount':'ts', 'avg_amt#close':'te'}, inplace = True)
    feat3 = r1.copy()
    
    return feat0,feat1,feat2,feat3

def process_sig_ext10_init_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $feats_root/stock_basic_oneminute/
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext10/
    """
    del_rank = 10
    min_bar = 241*0.6
    complete_tag = 0.6

    bar_name="stock_basic_oneminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, bar_name)
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/init.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    feat0,feat1,feat2,feat3 = calc_amt(cur_date,min_bar,del_rank,bar_folder,bar_name,daily_folder)
    #sr
    feat4 = calc_sr(cur_date, lag, complete_tag, all_date_list, bar_folder, bar_name)
    
    res = pd.merge(feat1, feat0, on='id', how='outer')
    res = pd.merge(res, feat2, on='id', how='outer')
    res = pd.merge(res, feat3, on='id', how='outer')
    res = pd.merge(res, feat4, on='id', how='outer')
    col_list = list(res.columns)
    col_list.remove('id')
    col_list = ['id']+col_list
    if len(res)>0:
        res[col_list].to_csv(out_file, index=False)

def process_sig_ext10_init(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
    output:
        $output_dir/eod.sig.ext10/
    """
    out_folder = '%s/eod.sig.ext10/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(19000101, end_date)
    prev_date_list = get_date_period(begin_date, lag, all_date_list)
    begin_date = prev_date_list[0]
    date_list = [x for x in all_date_list if x>=begin_date]
    logging.info('process sig ext10 init')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext10_init_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext10_init_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext10_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $output_dir/eod.sig.ext10/init.$date.csv
    output:
        $output_dir/eod.sig.ext10/basic.$date.csv
    """
    complete_tag = 0.6
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    pick_list = get_date_period(cur_date, lag, all_date_list)
    ori_set = []
    for cur_pick in pick_list:
        cur_file = '%s/init.%s.csv'%(out_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = pd.read_csv(cur_file)
        res['tdate'] = cur_pick
        ori_set.append(res)
    ori_set = pd.DataFrame().append(ori_set)
    #剔除交易时间不够的股票
    r1 = ori_set.groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    ori_set = ori_set[ori_set['id'].isin(r1['id'])]
    if len(res)==0:
        return

    col_list = list(ori_set.columns)
    col_list.remove('id')
    col_list.remove('sr')
    col_list.remove('tdate')
    res = ori_set.groupby(['id'],as_index=False)[col_list].mean()
    ori_set = ori_set[ori_set['tdate']==cur_date]
    res = pd.merge(res, ori_set[['id','sr']], on='id', how='outer')
    res.to_csv(out_file, index=False)

def process_sig_ext10(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
    output:
        $output_dir/eod.sig.ext10/
    """
    out_folder = '%s/eod.sig.ext10/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]

    logging.info('process sig ext10')
    if job_num==0:
        for cur_date in date_list:
            process_sig_ext10_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext10_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def kaiyuan(begin_date, end_date, output_dir, job_num):

    #日度理想反转
    process_sig_ext1(begin_date, end_date, output_dir, job_num)
    #微观理想反转
    process_sig_ext2_init(begin_date, end_date, output_dir, job_num, lag=20)
    process_sig_ext2(begin_date, end_date, output_dir, job_num, lag=20)
    #聪明钱
    process_sig_ext3(begin_date, end_date, output_dir, job_num)
    #改进apm
    process_sig_ext4(begin_date, end_date, output_dir, job_num)
    #交易者行为
    process_sig_ext5(begin_date, end_date, output_dir, job_num)
    #理想振幅
    process_sig_ext6(begin_date, end_date, output_dir, job_num)
    #主动买卖
    process_sig_ext7(begin_date, end_date, output_dir, job_num)
    #大单与小单资金流的alpha能力
    process_sig_ext8(begin_date, end_date, output_dir, job_num)
    #资金流动力学
    process_sig_ext9(begin_date, end_date, output_dir, job_num)
    #主力行为刻画
    process_sig_ext10_init(begin_date, end_date, output_dir, job_num, lag=20)
    process_sig_ext10(begin_date, end_date, output_dir, job_num, lag=20)

def process_sig_ext11_init_main(cur_date, out_folder):
    """
    input:
        $feats_root/stock_basic_fiveminute/
        $alpha_root/prod.trans.bar.5/
    output:
        $output_dir/eod.sig.ext11/
    """
    bar_name="stock_basic_fiveminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, bar_name)
    trans_folder = '%s/prod.trans.bar.5/'%(cfg.param_path_root)
    out_file = '%s/init.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    #使用分钟数据计算
    cur_file = '%s/%s.%s.csv'%(bar_folder, bar_name, cur_date)
    if not os.path.exists(cur_file):
        return
    bar_set = pd.read_csv(cur_file)
    bar_set.rename(columns={"min":"bar", "dealcnt":"trdcount"}, inplace=True)
    res = bar_set.groupby(['id'],as_index=False)[['volume']].sum()
    res.columns = ['id','total_vol']
    res = res[res['total_vol']>0]
    bar_set = pd.merge(bar_set, res, on='id')
    bar_set['w'] = bar_set['volume']/bar_set['total_vol']
    bar_set['ret'] = np.abs(bar_set['close']/bar_set['pclose']-1)*100
    bar_set.fillna(0, inplace=True)
    bar_set = bar_set[['id','w','ret']]
    
    final_set = None
    pow_list = [0.5,1,2,4,8]
    for cur_pow in pow_list:
        bar_set['sig'] = bar_set['w']*np.power(bar_set['ret'], cur_pow)
        res = bar_set.groupby(['id'],as_index=False)[['sig']].sum()
        res.columns = ['id','vwpin_bar_%s'%(int(cur_pow*10))]
        if final_set is None:
            final_set = res
        else:
            final_set = pd.merge(final_set, res, on='id')
    
    #使用逐笔数据计算
    cur_file = '%s/%s.csv'%(trans_folder, cur_date)
    if not os.path.exists(cur_file):
        final_set['vwpin_trans'] = np.nan
        final_set.to_csv(out_file, index=False)
        return
    if os.path.getsize(cur_file)<10:
        final_set['vwpin_trans'] = np.nan
        final_set.to_csv(out_file, index=False)
        return
    
    trans_set = pd.read_csv(cur_file)
    trans_set['count'] = trans_set['buyordercnt']+trans_set['sellordercnt']
    trans_set = trans_set[trans_set['count']>0]
    trans_set['prop'] = np.abs(trans_set['buyordercnt']-trans_set['sellordercnt'])/trans_set['count']
    trans_set['vol'] = trans_set['buyvolume']+trans_set['sellvolume']
    res = trans_set.groupby(['id'],as_index=False)[['vol']].sum()
    res.columns = ['id','total_vol']
    res = res[res['total_vol']>0]
    trans_set = pd.merge(trans_set, res, on='id')
    trans_set['w'] = trans_set['vol']/trans_set['total_vol']
    trans_set['sig'] = trans_set['w']*trans_set['prop']
    res = trans_set.groupby(['id'],as_index=False)[['sig']].sum()
    res.columns = ['id','vwpin_trans']
    final_set = pd.merge(final_set, res, on='id', how='outer')
    final_set.to_csv(out_file, index=False)

def process_sig_ext11_init(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
    output:
        $output_dir/eod.sig.ext11/
    """
    out_folder = '%s/eod.sig.ext11/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(19000101, end_date)
    prev_date_list = get_date_period(begin_date, lag, all_date_list)
    begin_date = prev_date_list[0]
    date_list = [x for x in all_date_list if x>=begin_date]
    logging.info('process sig ext11 init')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext11_init_main(cur_date, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext11_init_main)(cur_date, out_folder) for cur_date in date_list)

def process_sig_ext11_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $output_dir/eod.sig.ext11/init.$date.csv
    output:
        $output_dir/eod.sig.ext11/basic.$date.csv
    """
    complete_tag = 0.6
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    pick_list = get_date_period(cur_date, lag, all_date_list)
    ori_set = []
    for cur_pick in pick_list:
        cur_file = '%s/init.%s.csv'%(out_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = pd.read_csv(cur_file)
        res['tdate'] = cur_pick
        ori_set.append(res)
    ori_set = pd.DataFrame().append(ori_set)
    #剔除交易时间不够的股票
    r1 = ori_set.groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    ori_set = ori_set[ori_set['id'].isin(r1['id'])]
    if len(res)==0:
        return

    col_list = list(ori_set.columns)
    col_list.remove('id')
    col_list.remove('tdate')
    res = ori_set.groupby(['id'],as_index=False)[col_list].mean()
    res.to_csv(out_file, index=False)

def process_sig_ext11(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
    output:
        $output_dir/eod.sig.ext11/
    """

    out_folder = '%s/eod.sig.ext11/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]

    logging.info('process sig ext11')
    if job_num==0:
        for cur_date in date_list:
            process_sig_ext11_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext11_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def zhaoshang(begin_date, end_date, output_dir, job_num):
    #vwpin
    process_sig_ext11_init(begin_date, end_date, output_dir, job_num, 20)
    process_sig_ext11(begin_date, end_date, output_dir, job_num, 20)

def ext12_1(cur_date, all_date_list, basic_folder, day_folder):
    """
    input:
        $alpha_root/eod.mschars/
        $alpha_root/prod.stock.daily/
    input date_range: look back 1 year
        [$cur_date - 1year, $cur_date]
    output:
        $output_dir/eod.sig.ext12
    """

    blank_res = pd.DataFrame(columns=['id', 'rev1', 'mom1', 'dwf', 'dlf'])
    complete_tag = 0.6
    N = 80
    L = 120
    h = 10
    a = 1-np.power(np.exp(1),np.log(0.5)/h)
    
    pre_month = util_date.add_month(cur_date, -1)
    pre_year = cur_date-10000
    pick_list = util_date.get_spec_trade_date(pre_year, cur_date)
    basic_set = []
    day_set = []
    for cur_pick in pick_list:	
        cur_file = '%s/basic.%s.feather'%(basic_folder, cur_pick)
        if not os.path.exists(cur_file):
            return blank_res
        res = fe.read_feather(cur_file)
        basic_set.append(res[['tdate','id','touchup']])
        cur_file = '%s/basic.%s.feather'%(day_folder, cur_pick)
        res = fe.read_feather(cur_file)
        day_set.append(res)
    
    basic_set = pd.DataFrame().append(basic_set)
    basic_set = basic_set[basic_set['touchup']==1]
    day_set = pd.DataFrame().append(day_set)

    day_set['lnret'] = np.log(day_set['close']/day_set['pclose'])
    day_set = pd.merge(day_set[['tdate','id','lnret']], basic_set, on=['tdate','id'], how='left')
    #过去一个月的收益率，剔除触及涨停
    rev1 = day_set[(day_set['tdate']>pre_month) & (day_set['touchup']!=1)].groupby(['id'],as_index=False)[['lnret']].agg(['mean','count'])
    rev1.reset_index(inplace=True)
    rev1.columns = ['id','rev1','count']
    rev1 = rev1[rev1['count']>=max(rev1['count'])*complete_tag]
    #过去一年的收益率，剔除最近一个月和触及涨停
    mom1 = day_set[(day_set['tdate']<=pre_month) & (day_set['touchup']!=1)].groupby(['id'],as_index=False)[['lnret']].agg(['mean','count'])
    mom1.reset_index(inplace=True)
    mom1.columns = ['id','mom1','count']
    mom1 = mom1[mom1['count']>=max(mom1['count'])*complete_tag]

    pick_list = get_date_period(cur_date, L, all_date_list)
    weight = pd.DataFrame({'tdate':pick_list})
    weight['rank'] = weight['tdate'].rank(ascending=False)-1
    weight['w'] = np.power(1-a, weight['rank'])
    
    res = day_set[day_set['tdate']>=min(pick_list)].copy()
    r1 = res.groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=L*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    res['win'] = res.groupby(['tdate'],as_index=False)[['lnret']].rank(ascending=False)
    res['lose'] = res.groupby(['tdate'],as_index=False)[['lnret']].rank()
    dwf = res[res['win']<=N]
    dwf = pd.merge(dwf, weight, on=['tdate'])
    dwf = dwf.groupby(['id'],as_index=False)[['w']].sum()
    dwf['dwf'] = np.power(dwf['w'], 0.5)
    dlf = res[res['lose']<=N]
    dlf = pd.merge(dlf, weight, on=['tdate'])
    dlf = dlf.groupby(['id'],as_index=False)[['w']].sum()
    dlf['dlf'] = np.power(dlf['w'], 0.5)
    
    res = pd.merge(rev1[['id','rev1']], mom1[['id','mom1']], on='id', how='outer')
    res = pd.merge(res, dwf[['id','dwf']], on='id', how='outer')
    res = pd.merge(res, dlf[['id','dlf']], on='id', how='outer')
    return res

def ext12_2(cur_date, all_date_list, basic_folder, day_folder):
    """
    input:
        $alpha_root/eod.mschars/
        $alpha_root/prod.stock.daily/
    input date_range: 
        [$cur_date - 23days, $cur_date]
    output:
        $output_dir/eod.sig.ext12
    """

    blank_res = pd.DataFrame(columns=['id', 'adj_to', 'price_delay'])
    complete_tag = 0.6
    L_to = 20
    L_delay = 3
    pick_list = get_date_period(cur_date, L_to+L_delay, all_date_list)
    pre_date = all_date_list[all_date_list.index(cur_date)-L_to+1]
    all_set = []
    for cur_pick in pick_list:	
        cur_file = '%s/basic.%s.feather'%(day_folder, cur_pick)
        if not os.path.exists(cur_file):
            return blank_res
        res = fe.read_feather(cur_file)
        all_set.append(res)
    all_set = pd.DataFrame().append(all_set)
    day_set = all_set[all_set['tdate']>=pre_date]
    #市值调整换手
    r1 = day_set.groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=L_to*complete_tag]
    day_set = day_set[day_set['id'].isin(r1['id'])]
    
    cur_set = day_set[day_set['tdate']==cur_date]
    res = res[res['to']>0].groupby(['id'],as_index=False)[['to']].mean()
    res = pd.merge(res, cur_set[['id','mv']], on='id')
    res['to'] = np.log(res['to'])
    res['mv'] = np.log(res['mv'])
    res['adj_to'] = calc_reg_normal(res['to'].values, res['mv'].values)
    
    #价格时滞
    temp_set = all_set.loc[all_set['id'].isin(cur_set['id']),['tdate','id','tyield','mv']]
    r1 = temp_set.groupby(['tdate'],as_index=False)[['mv']].sum()
    r1.columns = ['tdate','total_mv']
    temp_set = pd.merge(temp_set, r1, on='tdate')
    temp_set['mkt'] = temp_set['tyield']*temp_set['mv']/temp_set['total_mv']
    mkt = temp_set.groupby(['tdate'],as_index=False)[['mkt']].sum()
    mkt.sort_values(['tdate'], inplace=True)
    for i in range(L_delay):
            mkt['mkt%s'%(i+1)] = mkt['mkt'].shift(i+1)
    mkt = mkt[mkt['tdate']>=pre_date]
    del mkt['tdate']
    temp_set = temp_set[temp_set['tdate']>=pre_date]
    r1 = temp_set.pivot_table(index='tdate',columns='id',values='tyield')
    
    price_delay = []
    for i in range(r1.shape[1]):
            y = r1.iloc[:,i].values
            #所有值相等
            if len(set(y)-set([np.nan]))==1:
                    price_delay.append(np.nan)
            else:
                    x = mkt['mkt'].values
                    x = sm.add_constant(x)
                    model = sm.OLS(y, x).fit()
                    r_cur = model.rsquared
                    x = mkt.values
                    x = sm.add_constant(x)
                    model = sm.OLS(y, x).fit()
                    r_all = model.rsquared
                    price_delay.append(1-r_cur/r_all)
    price_delay = pd.DataFrame({'id':r1.columns,'price_delay':price_delay})
    res = pd.merge(res[['id','adj_to']], price_delay, on='id', how='outer')
    return res

def slow_corr(ret_set, id_list, date_list, L_adj, L_cor, N, all_set):
	aaa = time.time()
	bias = []
	for i in range(10):
		res = ret_set[i:(i+L_cor)]
		temp_date= max(res.index)
		res = res.corr()
		res.reset_index(inplace=True)
		col_list = list(res.columns)
		col_list = [str(x) for x in col_list]
		res.columns = col_list
		res = pd.melt(res, id_vars=['id'])
		res.columns = ['ori_id','id','corr']
		res[['ori_id','id']] = res[['ori_id','id']].astype(int)
		res = res[res['ori_id']!=res['id']]
		res['pos'] = res.groupby(['ori_id'],as_index=False)[['corr']].rank(ascending=False)
		res = res[res['pos']<=N]
		res['tdate'] = temp_date
		bias.append(res)
	yyy = pd.DataFrame().append(bias)
	zzz = pd.merge(all_set, yyy, on=['tdate','id'])
	zzz = zzz.groupby(['tdate','ori_id'])[['tyield']].sum()
	zzz.reset_index(inplace=True)
	print(time.time()-aaa)

def ext12_3(cur_date, all_date_list, basic_folder, day_folder):
    """
    input:
        $alpha_root/eod.mschars/
        $alpha_root/prod.stock.daily/
    input date_range: 
        [$cur_date - 310days, $cur_date]
    output:
        $output_dir/eod.sig.ext12
    """

    blank_res = pd.DataFrame(columns=['id','spread_bias_price','spread_bias_ret'])
    complete_tag = 0.7
    L_cor = 250
    L_adj = 60
    N = 10
    pick_list = get_date_period(cur_date, L_cor+L_adj-1, all_date_list)
    all_set = []
    for cur_pick in pick_list:	
        cur_file = '%s/basic.%s.feather'%(day_folder, cur_pick)
        if not os.path.exists(cur_file):
            return blank_res
        res = fe.read_feather(cur_file)
        all_set.append(res)
    all_set = pd.DataFrame().append(all_set)
    #剔除交易时间不够的股票
    r1 = all_set.groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=(L_cor+L_adj)*complete_tag]
    all_set = all_set[all_set['id'].isin(r1['id'])]
    
    ret_set = all_set.pivot_table(index='id',columns='tdate',values='tyield')
    ret_set.fillna(0, inplace=True)
    id_list = np.array(ret_set.index)
    date_list = np.array(ret_set.columns)
    ret_set = ret_set.values

    bias = []
    for i in range(L_adj):
            temp_date = date_list[L_cor+i-1]
            res = ret_set[:,i:(i+L_cor)]
            #删除全0行
            keep_idx = ~(res==0).all(1)
            res = res[keep_idx]
            res = np.corrcoef(res)
            res[np.diag_indices_from(res)] = -1
            pos_set = np.percentile(res, (1-N/len(id_list))*100, axis=0)
            pos_set = np.tile(pos_set, res.shape[0]).reshape(res.shape[0], -1).T
            res[res>=pos_set] = 1
            res[res<pos_set] = 0
            res = res.astype(int)
            temp_ret = ret_set[:,L_cor+i-1]
            temp_ret = temp_ret[keep_idx]
            temp_ret = np.tile(temp_ret, res.shape[0]).reshape(res.shape[0], -1)
            temp_ret = (res*temp_ret).mean(1)
            temp_ret = pd.DataFrame({'id':id_list[keep_idx], 'ret_bmk':temp_ret, 'tdate':temp_date})
            bias.append(temp_ret)
    bias = pd.DataFrame().append(bias)
    
    all_set['close'] = all_set['close']*all_set['adjfactor']
    bias = pd.merge(bias, all_set[['tdate','id','close','tyield']], on=['id','tdate'])
    bias['ret_bmk'] = bias['ret_bmk']+1
    bias['tyield'] = bias['tyield']+1
    bias['close_bmk'] = bias.groupby(['id'],as_index=False)[['ret_bmk']].cumprod()
    bias['spread_price'] = np.log(bias['close'])-np.log(bias['close_bmk'])
    bias['spread_ret'] = np.log(bias['tyield'])-np.log(bias['ret_bmk'])
    res = bias.groupby(['id'],as_index=False)[['spread_price','spread_ret']].agg(['mean','std'])
    res.reset_index(inplace=True)
    res.columns = ['id','price_mean','price_std','ret_mean','ret_std']
    res = pd.merge(res, bias.loc[bias['tdate']==cur_date, ['id','spread_price','spread_ret']], on='id')
    res['spread_bias_price'] = (res['spread_price']-res['price_mean'])/res['price_std']
    res['spread_bias_ret'] = (res['spread_ret']-res['ret_mean'])/res['ret_std']
    return res[['id','spread_bias_price','spread_bias_ret']]

def ext12_4(cur_date, all_date_list, basic_folder, day_folder):
    """
    input:
        $alpha_root/eod.mschars/
        $alpha_root/prod.stock.daily/
    input date_range: 
        [$cur_date - 20days, $cur_date]
    output:
        $output_dir/eod.sig.ext12
    """

    blank_res = pd.DataFrame(columns=['id', 'arpp5d', 'apb5d', 'arpp20d', 'apb20d'])
    #src_folder = '%s/eod.mschar.combine'%(cfg.param_path_root)
    src_folder = basic_folder
    cur_file = '%s/%s.csv'%(src_folder, cur_date)
    if not os.path.exists(cur_file):
        return blank_res
    
    temp_set = []
    pick_list = get_date_period(cur_date, 20, all_date_list)
    for cur_pick in pick_list:
            cur_file = '%s/%s.csv'%(src_folder, cur_pick)
            if os.path.exists(cur_file):
                    res = pd.read_csv(cur_file)
                    res['tdate'] = cur_pick
                    temp_set.append(res)
    temp_set = pd.DataFrame().append(temp_set)
    r1 = temp_set.loc[temp_set['tdate']==cur_date, ['id','arpp5d','apb5d']]
    r2 = temp_set.groupby(['id'],as_index=False)[['arpp','apb']].mean()
    r2.columns = ['id','arpp20d','apb20d']
    r1 = pd.merge(r1,r2,on='id',how='outer')
    return r1

def ext12_5(cur_date, all_date_list, basic_folder, day_folder, out_folder):
    """
    input:
        $alpha_root/eod.mschars/
        $alpha_root/prod.stock.daily/
        $output_dir/eod.sig.ext12/init.$date.csv
    input date_range: 
        [$cur_date - 9month, $cur_date]
    output:
        $output_dir/eod.sig.ext12
    """

    blank_res = pd.DataFrame(columns=['id', 'mild_ret_9m', 'extreme_ret_1m'])
    complete_tag = 0.6
    cur_file = '%s/init.%s.csv'%(out_folder, cur_date)
    if not os.path.exists(cur_file):
        return blank_res
    
    temp_set = []
    mom_date = util_date.add_month(cur_date, -9)
    rev_date = util_date.add_month(cur_date, -1)
    pick_list = util_date.get_spec_trade_date(mom_date, cur_date)
    for cur_pick in pick_list:
            cur_file = '%s/init.%s.csv'%(out_folder, cur_pick)
            if os.path.exists(cur_file):
                    res = pd.read_csv(cur_file)
                    res['tdate'] = cur_pick
                    temp_set.append(res)
    temp_set = pd.DataFrame().append(temp_set)
    r1 = temp_set.groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=max(r1['tdate'])*complete_tag]
    mom_set = temp_set[temp_set['id'].isin(r1['id'])]
    mom_set = mom_set.groupby(['id'],as_index=False)[['mild']].mean()
    mom_set.columns = ['id','mild_ret_9m']
    temp_set = temp_set[temp_set['tdate']>=rev_date]
    r1 = temp_set.groupby(['id'],as_index=False)[['tdate']].count()
    r1 = r1[r1['tdate']>=max(r1['tdate'])*complete_tag]
    rev_set = temp_set[temp_set['id'].isin(r1['id'])]
    rev_set = rev_set.groupby(['id'],as_index=False)[['extreme']].mean()
    rev_set.columns = ['id','extreme_ret_1m']
    r2 = pd.merge(mom_set, rev_set, on='id', how='outer')
    return r2

def ext12_6(cur_date, all_date_list, basic_folder, day_folder):
    """
    input:
        $alpha_root/eod.l2chars.combine/
        $alpha_root/eod.mschars/
        $alpha_root/prod.stock.daily/
    input date_range: 
        [$cur_date - 20days, $cur_date]
    output:
        $output_dir/eod.sig.ext12
    """

    blank_res = pd.DataFrame(columns=['id','premkt_drop','b2s_order','b2s_trans','diversity_absolute','diversity_relative','apb_trans','apb_n1','apb_n2','apb_n3','bo_buy_p1','bo_buy_p30','early_bo_buy_p1','early_bo_buy_p30','bo_in_p1','bo_in_p30','early_bo_in_p30','bo_ret_p1','bo_ret_p30','early_bo_ret_p1'])
    #src_folder = '%s/eod.l2chars.combine'%(cfg.param_path_root)
    src_folder = '%s/eod.l2chars'%(cfg.param_path_root)
    cur_file = '%s/%s.csv'%(src_folder, cur_date)
    if not os.path.exists(cur_file):
        return blank_res
    
    temp_set = []
    pick_list = get_date_period(cur_date, 20, all_date_list)
    for cur_pick in pick_list:
        cur_file = '%s/%s.csv'%(src_folder, cur_pick)
        if os.path.exists(cur_file):
            res = pd.read_csv(cur_file)
            res['tdate'] = cur_pick
            temp_set.append(res)
    temp_set = pd.DataFrame().append(temp_set)
    
    r1 = temp_set.groupby(['id'],as_index=False)[['l2c0','l2c1','l2c4','l2c2','l2c3','l2c5','l2c6','l2c7','l2c8']].mean()
    r1.columns = ['id','premkt_drop','b2s_order','b2s_trans','diversity_absolute','diversity_relative','apb_trans','apb_n1','apb_n2','apb_n3']
    
    r2 = temp_set.groupby(['id'],as_index=False)[['bopct_p1','bopct_p30','bopcth1_p1','bopcth1_p30','l2c10','l2c13','l2c12']].mean()
    r2.columns = ['id','bo_buy_p1','bo_buy_p30','early_bo_buy_p1','early_bo_buy_p30','bo_in_p1','bo_in_p30','early_bo_in_p30']
    
    r3 = temp_set.groupby(['id'],as_index=False)[['aret_p1','aret_p30','aret1h_p1']].mean()
    r3.columns = ['id','bo_ret_p1','bo_ret_p30','early_bo_ret_p1']
    
    res = pd.merge(r1, r2, on='id', how='outer')
    res = pd.merge(res, r3, on='id', how='outer')
    return res

def process_sig_ext12_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $alpha_root/eod.mschars/
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext12
    """

    basic_folder = '%s/eod.mschars/'%(cfg.param_path_root)
    day_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    #涨跌停、涨跌幅
    r1 = ext12_1(cur_date, all_date_list, basic_folder, day_folder)
    #价格时滞、调整换手
    r2 = ext12_2(cur_date, all_date_list, basic_folder, day_folder)
    #价差偏离
    r3 = ext12_3(cur_date, all_date_list, basic_folder, day_folder)
    #量价关系&时间尺度的买卖压力
    r4 = ext12_4(cur_date, all_date_list, basic_folder, day_folder)
    #温和动量&极端反转
    r5 = ext12_5(cur_date, all_date_list, basic_folder, day_folder, out_folder)
    #委托订单&大单
    r6 = ext12_6(cur_date, all_date_list, basic_folder, day_folder)

    res = pd.merge(r1, r2, on='id', how='outer')
    res = pd.merge(res, r3, on='id', how='outer')
    res = pd.merge(res, r4, on='id', how='outer')
    res = pd.merge(res, r5, on='id', how='outer')
    res = pd.merge(res, r6, on='id', how='outer')
    if len(res)>0:
        res.to_csv(out_file, index=False)

def process_sig_ext12(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext12
    """
    out_folder = '%s/eod.sig.ext12/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext12')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext12_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext12_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext12_init_main(cur_date, out_folder):
    """
    input:
        $feats_dir/stock_basic_fiveminute/
    output:
        $output_dir/eod.sig.ext12/
    """
    bar_name="stock_basic_fiveminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, bar_name)
    out_file = '%s/init.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    cur_file = '%s/%s.%s.csv'%(bar_folder, bar_name, cur_date)
    if not os.path.exists(cur_file):
        return
    bar_set = pd.read_csv(cur_file)
    bar_set.rename(columns={"min":"bar", "dealcnt":"trdcount"}, inplace=True)
    bar_set['logr'] = np.log(bar_set['close']/bar_set['pclose'])
    bar_set['logr'] = bar_set['logr'].fillna(0)
    
    overnight_set = bar_set.loc[bar_set['bar']==930, ['id','bar','logr']]
    overnight_set['extreme'] = 0
    
    intra_set = bar_set.loc[bar_set['bar']>930, ['id','bar','logr']]
    res = intra_set.groupby(['id'],as_index=False)[['logr']].median()
    res.columns = ['id','median']
    intra_set = pd.merge(intra_set, res, on='id')
    intra_set['MAD'] = abs(intra_set['logr']-intra_set['median'])
    res = intra_set.groupby(['id'],as_index=False)[['MAD']].median()
    del intra_set['MAD']
    intra_set = pd.merge(intra_set, res, on='id')
    intra_set['extreme'] = 0
    intra_set.loc[intra_set['logr']>=(intra_set['median']+1.96*1.483*intra_set['MAD']), 'extreme'] = 1
    intra_set.loc[intra_set['logr']<=(intra_set['median']-1.96*1.483*intra_set['MAD']), 'extreme'] = 1
    intra_set = intra_set[overnight_set.columns].append(overnight_set)
    
    res = intra_set.groupby(['id','extreme'],as_index=False)[['logr']].sum()
    res.loc[res['extreme']==1,'extreme'] = 'extreme'
    res.loc[res['extreme']==0,'extreme'] = 'mild'
    res = res.pivot_table(index='id',columns='extreme',values='logr')
    res.reset_index(inplace=True)
    res.fillna(0,inplace=True)
    res.to_csv(out_file, index=False)

def process_sig_ext12_init(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
    output:
        $output_dir/eod.sig.ext12
    """
    out_folder = '%s/eod.sig.ext12/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)

    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    prev_date_list = get_date_period(begin_date, lag, all_date_list)
    begin_date = prev_date_list[0]
    date_list = [x for x in all_date_list if x>=begin_date]

    logging.info('process sig ext12 init')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext12_init_main(cur_date, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext12_init_main)(cur_date, out_folder) for cur_date in date_list)

def dongfang(begin_date, end_date, output_dir, job_num):
    process_sig_ext12_init(begin_date, end_date, output_dir, job_num, lag=250)
    process_sig_ext12(begin_date, end_date, output_dir, job_num)

def process_sig_ext13_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext13/
    """

    complete_tag = 0.6
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, lag, all_date_list)
    mf_set = get_moneyflow(min(pick_list), max(pick_list))
    r1 = mf_set[mf_set['tdate']==cur_date]
    if len(r1)==0:
        return
    r1 = mf_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    mf_set = mf_set[mf_set['id'].isin(r1['id'])]
    if len(mf_set)==0:
        return
    
    day_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = fe.read_feather(cur_file)
        day_set.append(res)
    day_set = pd.DataFrame().append(day_set)
    
    mf_set = pd.merge(mf_set[['tdate','id','a_s','amount']], day_set[['tdate','id','tyield','mv']], on=['tdate','id'])
    #小单交易占比
    mf_set['so_p'] = mf_set['a_s']/mf_set['amount']/2
    #指数涨跌幅
    res = mf_set.groupby(['tdate'],as_index=False)[['mv']].sum()
    res.columns = ['tdate','total_mv']
    mf_set = pd.merge(mf_set, res, on=['tdate'])
    mf_set['weight'] = mf_set['mv']/mf_set['total_mv']
    mf_set['tyield_bmk'] = mf_set['weight']*mf_set['tyield']
    res = mf_set.groupby(['tdate'],as_index=False)[['tyield_bmk']].sum()
    del mf_set['tyield_bmk']
    mf_set = pd.merge(mf_set, res, on=['tdate'])
    #指数小单交易占比
    res = mf_set.groupby(['tdate'],as_index=False)[['a_s','amount']].sum()
    res['so_p_bmk'] = res['a_s']/res['amount']/2
    mf_set = pd.merge(mf_set, res[['tdate','so_p_bmk']], on=['tdate'])
    #超额
    mf_set['tyield_act'] = mf_set['tyield']-mf_set['tyield_bmk']
    mf_set['so_p_act'] = mf_set['so_p']-mf_set['so_p_bmk']
    
    final_set = None
    check_list = [['so_p','tyield'],['so_p','tyield_act'],['so_p_act','tyield'],['so_p_act','tyield_act']]
    for cur_check in check_list:
        p_so = cur_check[0]
        p_ret = cur_check[1]
        res = mf_set.groupby(['id'],as_index=False)[[p_so]].quantile([0.2,0.8])
        res.reset_index(inplace=True)
        res.loc[res['level_1']==0.2,'level_1'] = 'p1'
        res.loc[res['level_1']==0.8,'level_1'] = 'p5'
        res = res.pivot_table(index='id',columns='level_1',values=p_so)
        temp_set = pd.merge(mf_set, res, on='id')
        p1 = temp_set[temp_set[p_so]<=temp_set['p1']].groupby(['id'],as_index=False)[[p_ret]].mean()
        p1.columns = ['id','p1']
        p5 = temp_set[temp_set[p_so]>=temp_set['p5']].groupby(['id'],as_index=False)[[p_ret]].mean()
        p5.columns = ['id','p5']
        temp_set = pd.merge(p1, p5, on='id')
        temp_set['%s_%s'%(p_so,p_ret)] = temp_set['p1']-temp_set['p5']
        if final_set is None:
            final_set = temp_set[['id','%s_%s'%(p_so,p_ret)]]
        else:
            final_set = pd.merge(final_set, temp_set[['id','%s_%s'%(p_so,p_ret)]], on='id',how='outer')
    res.to_csv(out_file)

def process_sig_ext13(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext13/
    """
    out_folder = '%s/eod.sig.ext13/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext13')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext13_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext13_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def dongwu(begin_date, end_date, output_dir, job_num):
    process_sig_ext13(begin_date, end_date, output_dir, job_num)

def process_sig_ext14_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $output_dir/eod.sig.ext14/init.$date.csv
    output:
        $output_dir/eod.sig.ext14/basic.$date.csv
    """

    complete_tag = 0.6
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, lag, all_date_list)
    day_set = []
    for cur_pick in pick_list:
        cur_file = '%s/init.%s.csv'%(out_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = pd.read_csv(cur_file)
        res['tdate'] = cur_pick
        day_set.append(res)
    day_set = pd.DataFrame().append(day_set)
    r1 = day_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    day_set = day_set[day_set['id'].isin(r1['id'])]
    if len(day_set)==0:
        return
    
    col_list = list(day_set.columns)
    col_list.remove('id')
    col_list.remove('tdate')
    res = day_set.groupby(['id'],as_index=False)[col_list].mean()
    col_list = ['%s_%sd'%(x,lag) for x in col_list]
    col_list = ['id']+col_list
    res.columns = col_list
    res.to_csv(out_file, index=False)

def process_sig_ext14(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
    output:
        $output_dir/eod.sig.ext14/
    """
    out_folder = '%s/eod.sig.ext14/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    logging.info('process sig ext14')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext14_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext14_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext14_init_main(cur_date, out_folder):
    """
    input:
        $feats_root/stock_basic_oneminute/
    output:
        $output_dir/eod.sig.ext14/
    """
    N_in = 0.2
    N_mom = 0.3
    bar_name = "stock_basic_oneminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, bar_name)
    out_file = '%s/init.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return

    cur_file = '%s/%s.%s.csv'%(bar_folder, bar_name, cur_date)
    if not os.path.exists(cur_file):
        return
    bar_set = pd.read_csv(cur_file)
    bar_set.rename(columns={"min":"bar", "dealcnt":"trdcount"}, inplace=True)
    if sum(bar_set['trdcount'])==0:
        return
    bar_set['ret'] = bar_set['close']/bar_set['pclose']-1
    bar_set['lnr'] = np.log(bar_set['close']/bar_set['pclose'])
    bar_set['amt_per_trd'] = util_other.my_div(bar_set['amount'], bar_set['trdcount'])
    bar_set['tag'] = 0
    bar_set.loc[bar_set['ret']>0,'tag'] = 1
    bar_set.loc[bar_set['ret']<0,'tag'] = -1
    
    #平均单笔成交金额类
    amt_all = bar_set.groupby(['id'],as_index=False)[['amount','trdcount']].sum()
    amt_all.loc[amt_all['trdcount']==0, 'trdcount'] = np.nan
    amt_all['amt_all'] = amt_all['amount']/amt_all['trdcount']
    
    amt_in = bar_set[bar_set['tag']==1].groupby(['id'],as_index=False)[['amount','trdcount']].sum()
    amt_in.loc[amt_in['trdcount']==0, 'trdcount'] = np.nan
    amt_in['amt_in'] = amt_in['amount']/amt_in['trdcount']
    
    amt_out = bar_set[bar_set['tag']==-1].groupby(['id'],as_index=False)[['amount','trdcount']].sum()
    amt_out.loc[amt_out['trdcount']==0, 'trdcount'] = np.nan
    amt_out['amt_out'] = amt_out['amount']/amt_out['trdcount']
    
    amt_all = pd.merge(amt_all[['id','amt_all']], amt_in[['id','amt_in']], on='id', how='outer')
    amt_all = pd.merge(amt_all, amt_out[['id','amt_out']], on='id', how='outer')
    amt_all['in_ratio'] = amt_all['amt_in']/amt_all['amt_all']
    amt_all['out_ratio'] = amt_all['amt_out']/amt_all['amt_all']
    amt_all['net_ratio'] = amt_all['amt_in']/amt_all['amt_out']
    
    #大单资金流向
    #不考虑集合竞价
    bar_set['real_amt'] = bar_set['amount']*bar_set['tag']
    bar_set = bar_set[bar_set['bar']>930]
    r0 = bar_set.groupby(['id'],as_index=False)[['amount']].sum()
    temp_set = bar_set.groupby(['id'],as_index=False)[['amt_per_trd']].quantile([1-N_in, 1-N_mom])
    temp_set.reset_index(inplace=True)
    temp_set.loc[temp_set['level_1']==1-N_in,'level_1'] = 'N_in'
    temp_set.loc[temp_set['level_1']==1-N_mom,'level_1'] = 'N_mom'
    temp_set = temp_set.pivot_table(index='id',columns='level_1',values='amt_per_trd')
    bar_set = pd.merge(bar_set, temp_set, on='id')
    r1 = bar_set[bar_set['amt_per_trd']>=bar_set['N_in']]
    r1 = r1.groupby(['id'],as_index=False)[['real_amt']].sum()
    r1 = pd.merge(r1, r0, on='id')
    r1['bo_net_ratio'] = r1['real_amt']/r1['amount']
    
    r2 = bar_set[bar_set['amt_per_trd']>=bar_set['N_mom']]
    r2 = r2.groupby(['id'],as_index=False)[['lnr']].sum()
    r2.columns = ['id','bo_ret']
    
    amt_all = pd.merge(amt_all[['id','in_ratio','out_ratio','net_ratio']], r1[['id','bo_net_ratio']], on='id', how='outer')
    amt_all = pd.merge(amt_all, r2, on='id', how='outer')
    amt_all.to_csv(out_file, index=False)

def process_sig_ext14_init(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
    output:
        $output_dir/eod.sig.ext14/
    """
    out_folder = '%s/eod.sig.ext14/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    prev_date_list = get_date_period(begin_date, lag, all_date_list)
    begin_date = prev_date_list[0]
    date_list = [x for x in all_date_list if x>=begin_date]

    logging.info('process sig ext14 init')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext14_init_main(cur_date, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext14_init_main)(cur_date, out_folder) for cur_date in date_list)

def haitong(begin_date, end_date, output_dir, job_num):
    process_sig_ext14_init(begin_date, end_date, output_dir, job_num, 20)
    process_sig_ext14(begin_date, end_date, output_dir, job_num, 20)

def process_sig_ext15_main(cur_date, lag, all_date_list, out_folder):
    """
    input:
        $alpha_root/prod.stock.daily/
    output:
        $output_dir/eod.sig.ext15/ 
    """

    N_long = 60
    N_short = 10
    complete_tag = 0.6
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, N_long, all_date_list)
    day_set = []
    for cur_pick in pick_list:
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            return
        res = fe.read_feather(cur_file)
        day_set.append(res)

    day_set = pd.DataFrame().append(day_set)
    r1 = day_set.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    day_set = day_set[day_set['id'].isin(r1['id'])]
    if len(day_set)==0:
        return
    
    pick_list = get_date_period(cur_date, N_short, all_date_list)
    init_date = min(pick_list)
    day_set['lnr'] = np.log(day_set['tyield']+1)
    temp_set = day_set[day_set['tdate']>=init_date].groupby(['id'],as_index=False)[['lnr']].sum()
    temp_set['tag'] = np.power(np.exp(1), temp_set['lnr'])-1
    day_set['close'] = day_set['close']*day_set['adjfactor']
    res = day_set.groupby(['id'],as_index=False)[['close']].agg(['min','max'])
    res.reset_index(inplace=True)
    res.columns = ['id','min_p','max_p']
    res = pd.merge(res, day_set.loc[day_set['tdate']==cur_date, ['id','close']], on='id')
    res = pd.merge(res, temp_set[['id','tag']], on='id')
    res['anchor_rev'] = res['close']/res['min_p']-1
    idx = res['tag']<=0
    res.loc[idx, 'anchor_rev'] = res.loc[idx, 'close']/res.loc[idx, 'max_p']-1
    res[['id','anchor_rev']].to_csv(out_file, index=False)

def process_sig_ext15(begin_date, end_date, output_dir, job_num):
    """
    input:
    output:
        $output_dir/eod.sig.ext15/ 
    """
    out_folder = '%s/eod.sig.ext15/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    lag = 20
    logging.info('process sig ext15')
    if job_num==0:
        for cur_date in date_list:
            print(cur_date)
            process_sig_ext15_main(cur_date, lag, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext15_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def zhongyin(begin_date, end_date, output_dir, job_num):
    process_sig_ext15(begin_date, end_date, output_dir, job_num, )

def ext16_pvi(cur_date):
    blank_res = pd.DataFrame(columns=['id','high_pvi'])
    minbar="stock_basic_fiveminute"
    bar_file = '%s/%s/%s.%s.csv'%(cfg.param_path_src_minbar_file, minbar, minbar, cur_date)
    if not os.path.exists(bar_file):
        return blank_res

    res = pd.read_csv(bar_file)
    res.rename(columns={"min":"bar"}, inplace=True)
    res['lnret'] = np.log(res['close']/res['pclose'])
    #忽略集合竞价
    res = res[(res['bar']>930) & (res['bar']<1500)]
    res.sort_values(['id','bar'], inplace=True)
    res['pre_id'] = res['id'].shift(1)
    res['pre_vol'] = res['volume'].shift(1)
    res = res[(res['id']==res['pre_id']) & (res['volume']>res['pre_vol'])]
    res = res.groupby(['id'],as_index=False)[['lnret']].sum()
    res.columns = ['id','high_pvi']
    return res

def ext16_retv(cur_date, all_date_list):
    minbar="stock_basic_fiveminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, minbar)
    daily_folder = '%s/prod.stock.daily/'%(cfg.param_path_root)
    blank_res = pd.DataFrame(columns=['id','high_retv5m20'])
    bar_file = '%s/%s.%s.csv'%(bar_folder, minbar, cur_date)
    if not os.path.exists(bar_file):
        return blank_res
    
    lag = 20
    complete_tag = 0.6
    split = 0.1
    pick_list = get_date_period(cur_date, lag, all_date_list)
    bar_set = []
    day_set = []
    for cur_pick in pick_list:
        cur_file = '%s/%s.%s.csv'%(bar_folder, minbar, cur_pick)
        if not os.path.exists(cur_file):
            return blank_res
        res = pd.read_csv(cur_file)
        res.rename(columns={"min":"bar"}, inplace=True)
        res['tdate'] = cur_pick
        bar_set.append(res[['tdate','id','bar','close','pclose','volume']])
        
        cur_file = '%s/basic.%s.feather'%(daily_folder, cur_pick)
        if not os.path.exists(cur_file):
            return blank_res
        res = fe.read_feather(cur_file)
        day_set.append(res[['tdate','id','adjfactor']])
    bar_set = pd.DataFrame().append(bar_set)
    day_set = pd.DataFrame().append(day_set)
    res = pd.merge(day_set, bar_set, on=['tdate','id'])
    res = res[res['volume']>0]
    res['volume'] = res['volume']/res['adjfactor']
    #剔除交易时间不够的股票
    r1 = res.groupby(['id'],as_index=False)[['tdate']].nunique()
    r1 = r1[r1['tdate']>=lag*complete_tag]
    res = res[res['id'].isin(r1['id'])]
    if len(res)==0:
            return blank_res
    res['ret'] = np.log(res['close']/res['pclose'])
    res.sort_values(by=['id','volume'], inplace=True)
    res['cum_vol'] = res.groupby(['id'],as_index=False)[['volume']].cumsum()
    temp_set = res.groupby(['id'],as_index=False)[['cum_vol']].max()
    temp_set.columns = ['id','split']
    temp_set['split'] = temp_set['split']*split
    res = pd.merge(res, temp_set, on='id')
    res['tag'] = 0
    res.loc[res['cum_vol']<=res['split'], 'tag'] = 1
    idx = res['tag']==1
    res.loc[idx, 'volume'] = 1/res.loc[idx, 'volume']
    temp_set = res.groupby(['id','tag'],as_index=False)[['volume']].sum()
    temp_set = temp_set.pivot_table(index='id',columns='tag',values='volume')
    temp_set.reset_index(inplace=True)
    temp_set.columns = ['id','rev','mom']
    res = pd.merge(res, temp_set, on='id')
    res.loc[idx, 'volume'] = res.loc[idx, 'volume']/res.loc[idx, 'mom']
    res.loc[~idx, 'volume'] = res.loc[~idx, 'volume']/res.loc[~idx, 'rev']
    res['ret'] = res['ret']*res['volume']
    temp_set = res.groupby(['id','tag'],as_index=False)[['ret']].sum()
    temp_set = temp_set.pivot_table(index='id',columns='tag',values='ret')
    temp_set.reset_index(inplace=True)
    temp_set.columns = ['id','rev','mom']
    temp_set['high_retv5m20'] = temp_set['rev']-temp_set['mom']
    temp_set.dropna(inplace=True)
    return temp_set[['id','high_retv5m20']]

def ext16_illiq(cur_date, all_date_list):
    minbar="stock_basic_fiveminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, minbar)
    blank_res = pd.DataFrame(columns=['id','high_illq'])
    bar_file = '%s/%s.%s.csv'%(bar_folder, minbar, cur_date)
    if not os.path.exists(bar_file):
        return blank_res
    
    enlarge = 1000000
    bar_set = pd.read_csv(bar_file)
    bar_set['ret'] = np.log(abs(bar_set['close']/bar_set['pclose']-1)+1)
    res = bar_set.groupby(['id'],as_index=False)[['ret','amount']].sum()
    res['high_illq'] = res['ret']*enlarge/np.sqrt(res['amount'])
    return res[['id','high_illq']]

def ext16_utd(cur_date, all_date_list):
    minbar="stock_basic_oneminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, minbar)
    daily_folder = '%s/stockdata.extra/'%(cfg.param_path_root)
    blank_res = pd.DataFrame(columns=['id','minuamt_utd'])
    bar_file = '%s/%s.%s.csv'%(bar_folder, minbar, cur_date)
    if not os.path.exists(bar_file):
        return blank_res
    
    bar_set = pd.read_csv(bar_file)
    cur_file = '%s/%s.csv'%(daily_folder, cur_date)
    day_set = pd.read_csv(cur_file)
    res = pd.merge(bar_set, day_set[['id','cur_freeshr']], on=['id'])
    res = res[res['volume']>0]
    res['to'] = res['volume']/res['cur_freeshr']
    res = res.groupby(['id'],as_index=False)[['to']].std()
    res.columns = ['id','minuamt_utd']
    return res[['id','minuamt_utd']]

def ext16_peak(cur_date, all_date_list):
    minbar="stock_basic_oneminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, minbar)
    blank_res = pd.DataFrame(columns=['id','high_tpkcnt1m'])
    bar_file = '%s/%s.%s.csv'%(bar_folder, minbar, cur_date)
    if not os.path.exists(bar_file):
        return blank_res
    
    bar_set = pd.read_csv(bar_file)
    bar_set.rename(columns={"min":"bar"}, inplace=True)
    bar_set.dropna(inplace=True)
    bar_set['sn'] = bar_set.groupby(['id'],as_index=False)[['bar']].rank()
    res = bar_set.groupby(['id'],as_index=False)[['volume']].agg(['mean','std'])
    res.reset_index(inplace=True)
    res.columns = ['id','mean','std']
    res['tag'] = res['mean']+res['std']
    res = pd.merge(bar_set[['id','sn','volume']], res[['id','tag']], on=['id'])
    res = res[res['volume']>res['tag']]
    res.sort_values(by=['id','sn'], inplace=True)
    res['pre_id'] = res['id'].shift(1, fill_value=-1)
    res['pre_sn'] = res['sn'].shift(1, fill_value=-1)
    idx1 = res['sn']-res['pre_sn']>1
    idx2 = (res['id']!=res['pre_id'])
    res = res[idx1 | idx2]
    res = res.groupby(['id'],as_index=False)[['sn']].count()
    res.columns = ['id','high_tpkcnt1m']
    return res

def ext16_beta(cur_date, all_date_list):
    minbar="stock_basic_oneminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, minbar)
    blank_res = pd.DataFrame(columns=['id','high_retamtbeta1m'])
    bar_file = '%s/%s.%s.csv'%(bar_folder, minbar, cur_date)
    if not os.path.exists(bar_file):
        return blank_res
    
    div = 10000000
    bar_set = pd.read_csv(bar_file)
    bar_set.rename(columns={"min":"bar"}, inplace=True)
    #不考虑集合竞价
    bar_set = bar_set[(bar_set['bar']>930) & (bar_set['bar']<1500)]
    bar_set['ret'] = bar_set['close']/bar_set['pclose']-1
    bar_set['amount'] = bar_set['amount']*np.sign(bar_set['ret'])/div
    bar_ret_stock = bar_set.pivot_table(index='id',columns='bar',values='ret',dropna=False)
    bar_signamount_stock = bar_set.pivot_table(index='id',columns='bar',values='amount',dropna=False)
    bar_ret_stock.fillna(0, inplace=True)
    bar_signamount_stock.fillna(0, inplace=True)
    beta0, retbvol, ss_res, ss_res_new, r2 = eod_mschars.cr_reg_resr2_nan(bar_ret_stock, bar_signamount_stock)
    res = pd.DataFrame({'id':bar_ret_stock.index,'high_retamtbeta1m':retbvol})
    return res

def ext16_industry(cur_date, all_date_list):
    minbar="stock_basic_oneminute"
    bar_folder = '%s/%s/'%(cfg.param_path_src_minbar_file, minbar)
    ind_folder = '%s/industry.sector/'%(cfg.param_path_root)
    blank_res = pd.DataFrame(columns=['id','high_retamtbeta1m'])
    bar_file = '%s/%s.%s.csv'%(bar_folder, minbar, cur_date)
    if not os.path.exists(bar_file):
        return blank_res
    
    bar_set = pd.read_csv(bar_file)
    bar_set.rename(columns={"min":"bar"}, inplace=True)
    cur_file = '%s/%s.csv'%(ind_folder, cur_date)
    ind_info = pd.read_csv(cur_file)
    bar_set = pd.merge(bar_set, ind_info, on='id')
    bar_ind_amount = bar_set.groupby(['bar','lev1'],as_index=False)[['amount']].sum()
    bar_ind_amount.columns = ['bar','lev1','iamount']
    bar_set_ind = pd.merge(bar_set[['id','bar','amount','lev1']], bar_ind_amount, on=['bar','lev1'])
    bar_amount_stock = bar_set_ind.pivot_table(index='id',columns='bar',values='amount')
    bar_amount_ind = bar_set_ind.pivot_table(index='id',columns='bar',values='iamount')
    bar_amount_stock.fillna(0, inplace=True)
    bar_amount_ind.fillna(0, inplace=True)
    beta0, beta1, resid, residnew, r2 = eod_mschars.cr_reg_resr2_nan(bar_amount_stock, bar_amount_ind)
    
    bar_amount_stock_mean = bar_amount_stock.mean(1)
    bar_amount_stock_sum = bar_amount_stock.sum(1)
    bar_amount_stock_mean[bar_amount_stock_mean==0] = np.nan
    bar_amount_stock_sum[bar_amount_stock_sum==0] = np.nan
    beta0z = beta0/bar_amount_stock_mean
    resid_pos = np.sum(residnew*(residnew>0), axis=1)/bar_amount_stock_sum
    idx = bar_amount_stock.columns<1300
    resid_am = residnew[:, idx]
    resid_pm = residnew[:, ~idx]
    amresid = np.sum(resid_am, axis=1)/bar_amount_stock_sum
    pmresid = np.sum(resid_pm, axis=1)/bar_amount_stock_sum
    res = pd.DataFrame({'id':bar_amount_stock.index,'beta0z':beta0z, 'beta1':beta1, 'resid_pos':resid_pos, 'amresid':amresid, 'pmresid':pmresid, 'r2':r2})
    res.reset_index(drop=True, inplace=True)
    return res

def process_sig_ext16_init_main(cur_date, all_date_list, out_folder):
    out_file = '%s/init.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    r1 = ext16_pvi(cur_date)
    r2 = ext16_retv(cur_date, all_date_list)
    r3 = ext16_illiq(cur_date, all_date_list)
    r4 = ext16_utd(cur_date, all_date_list)
    r5 = ext16_peak(cur_date, all_date_list)
    r6 = ext16_beta(cur_date, all_date_list)
    r7 = ext16_industry(cur_date, all_date_list)
    res = pd.merge(r1, r2, on='id', how='outer')
    res = pd.merge(res, r3, on='id', how='outer')
    res = pd.merge(res, r4, on='id', how='outer')
    res = pd.merge(res, r5, on='id', how='outer')
    res = pd.merge(res, r6, on='id', how='outer')
    res = pd.merge(res, r7, on='id', how='outer')
    if len(res)>0:
        res.to_csv(out_file,index=False)

def process_sig_ext16_init(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
        $feats_root/stock_basic_fiveminute/
        $feats_root/stock_basic_oneminute/
        $alpha_root/prod.stock.daily/
        $alpha_root/industry.sector/
    input date_range:
        [begin_date, end_date]
    output:
        $output_dir/eod.sig.ext16/init.$date.csv
    output date_range:
        [$begin_date - lag, $end_date]
    """
    out_folder = '%s/eod.sig.ext16/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    prev_date_list = get_date_period(begin_date, lag, all_date_list)
    begin_date = prev_date_list[0]
    date_list = [x for x in all_date_list if x>=begin_date]
    logging.info('process sig ext16 init')
    if job_num==0:
        for cur_date in date_list:
            logging.info(cur_date)
            process_sig_ext16_init_main(cur_date, all_date_list, out_folder)
    else:
        Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext16_init_main)(cur_date, all_date_list, out_folder) for cur_date in date_list)

def process_sig_ext16_main(cur_date, lag, all_date_list, out_folder):
    out_file = '%s/basic.%s.csv'%(out_folder, cur_date)
    if os.path.exists(out_file):
        return
    
    pick_list = get_date_period(cur_date, lag, all_date_list)
    final_set = []
    for cur_pick in pick_list:
        cur_file = '%s/init.%s.csv'%(out_folder, cur_pick)
        if not os.path.exists(cur_file):
            continue
        res = pd.read_csv(cur_file)
        res['tdate'] = cur_pick
        final_set.append(res)
    final_set = pd.DataFrame().append(final_set)
    cur_set = final_set[final_set['tdate']==cur_date]
    if len(cur_set)==0:
        return
    r1 = final_set.groupby(['id'],as_index=False)[['high_illq','high_tpkcnt1m']].mean()
    r1.columns = ['id','high_illq20','high_tpkcnt1m20']
    r2 = final_set.groupby(['id'],as_index=False)[['high_retamtbeta1m','minuamt_utd']].agg(['mean','std'])
    r2.reset_index(inplace=True)
    r2.columns = ['id','beta_mean','beta_std','utd_mean','utd_std']
    r2['high_retamtbeta1m20'] = r2['beta_mean']/r2['beta_std']
    r2['minuamt_utd20'] = r2['utd_mean']/r2['utd_std']
    cur_set = pd.merge(cur_set, r1, on='id')
    cur_set = pd.merge(cur_set, r2[['id','high_retamtbeta1m20','minuamt_utd20']], on='id')
    cur_set.to_csv(out_file, index=False)

def process_sig_ext16(begin_date, end_date, output_dir, job_num, lag):
    """
    input:
        $output_dir/eod.sig.ext16/init.$date.csv
    input date_range:
        [$begin_date - lag, $end_date]
    output:
        $output_dir/eod.sig.ext16/basic.$date.csv
    output date_range:
        [$begin_date, $end_date]
    """

    out_folder = '%s/eod.sig.ext16/'%(output_dir)
    os.makedirs(out_folder, exist_ok=True)
    all_date_list = util_date.get_spec_trade_date(20000101, end_date)
    date_list = [x for x in all_date_list if x>=begin_date]
    logging.info('process sig ext16')
    if job_num==0:
        for cur_date in date_list:
            logging.info(cur_date)
            process_sig_ext16_main(cur_date, lag, all_date_list, out_folder)
    else:
            Parallel(n_jobs=job_num, backend="loky", verbose=param_verbose)(delayed(process_sig_ext16_main)(cur_date, lag, all_date_list, out_folder) for cur_date in date_list)

def supplement(begin_date, end_date, output_dir, job_num):
    process_sig_ext16_init(begin_date, end_date, output_dir, job_num, lag=20)
    process_sig_ext16(begin_date, end_date, output_dir, job_num, lag=20)

def process_basic_bs(start_date, end_date, output_dir, job_num):

    kaiyuan(start_date, end_date, output_dir, job_num)
    zhaoshang(start_date, end_date, output_dir, job_num)
    dongfang(start_date, end_date, output_dir, job_num)
    dongwu(start_date, end_date, output_dir, job_num)
    zhongyin(start_date, end_date, output_dir, job_num)
    haitong(start_date, end_date, output_dir, job_num)
    supplement(start_date, end_date, output_dir, job_num)

def remove_file():
	cur_date = util_date.get_cur_date()
	init_date = util_date.add_day(cur_date, -20)
	sig_list = os.listdir(cfg.param_path_root)
	sig_list = [x for x in sig_list if x.startswith('eod.sig.ext')]
	for cur_sig in sig_list:
		file_list = os.listdir('%s/%s/'%(cfg.param_path_root, cur_sig))
		file_list = pd.DataFrame({'name':file_list})
		file_list['date'] = file_list['name'].str.extract('([0-9]{8})')
		file_list = file_list[file_list['date']>=str(init_date)]
		for cur_file in file_list['name']:
			cur_file = '%s/%s/%s'%(cfg.param_path_root, cur_sig, cur_file)
			print(cur_file)
			os.remove(cur_file)

def main():
    parser = ArgumentParser(description="generate alpha signals", usage="")
    parser.add_argument("--start_date", help='start date', type=int, default=0)
    parser.add_argument("--end_date", help='end date', type=int, default=0)
    parser.add_argument("--output_dir", help='output directory root', default='')
    parser.add_argument("--job_num", help="process num to use", type=int, default=int(os.cpu_count()/2))
    parser.add_argument("--debug", help="debug mode, log intermediate data", action='store_true')
    parser.add_argument("--reproduce", help="reproduce mode, read intermediate data", action='store_true')

    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format='eod_sig %(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    assert args.output_dir
 
    global debug
    debug = args.debug
    reproduce = args.reproduce
    if args.debug:
        os.makedirs(cfg.debug_output_dir, exist_ok=True)

    logging.info("output_root={}, job_num={}, time range=({},{}] debug_mode is {}, reproduce_mode is {}".format(args.output_dir, args.job_num, args.start_date, args.end_date, "ON" if debug else "OFF", "ON" if reproduce else "OFF"))

    #remove_file()
    process_basic_bs(args.start_date, args.end_date, args.output_dir, args.job_num)
    
if __name__ == "__main__":
    main()
    #__debug_ext4()
    #__debug_ext4_2()
    #__debug_ext4_3()
