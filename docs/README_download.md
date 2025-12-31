# EOD因子数据下载说明

## 概述

`download_eod_data_2025.py` 脚本用于使用QMT下载2025年全年EOD因子计算所需的数据。

## 依赖

- Python 3.7+
- xtquant (QMT Python接口)
- tqdm (进度条显示)

安装依赖：
```bash
pip install xtquant tqdm
```

## 使用方法

### 基本用法

下载2025年全年所有数据（使用默认股票列表）：
```bash
python scripts/download_eod_data_2025.py
```

### 指定日期范围

```bash
python scripts/download_eod_data_2025.py --start_date 20250101 --end_date 20251231
```

### 指定股票列表

```bash
python scripts/download_eod_data_2025.py --stock_list 600000.SH 000001.SZ 600519.SH
```

### 指定下载周期

默认下载：日线(1d)、1分钟(1m)、5分钟(5m)、60分钟(60m)

只下载日线和1分钟数据：
```bash
python scripts/download_eod_data_2025.py --periods 1d 1m
```

### 同时下载指数数据

```bash
python scripts/download_eod_data_2025.py --download_index
```

## 参数说明

- `--start_date`: 开始日期，格式YYYYMMDD，默认20250101
- `--end_date`: 结束日期，格式YYYYMMDD，默认20251231
- `--stock_list`: 指定股票代码列表，格式如 "600000.SH"，多个用空格分隔
- `--periods`: 下载周期，可选值：1d, 1m, 5m, 60m，默认全部下载
- `--download_index`: 是否下载指数数据（沪深300、上证指数等）

## 数据说明

下载的数据包括：

1. **日线数据 (1d)**: 开高低收、成交量、成交额等
2. **1分钟数据 (1m)**: 用于计算高频因子
3. **5分钟数据 (5m)**: 用于计算中频因子
4. **60分钟数据 (60m)**: 用于计算低频因子

## 注意事项

1. **确保QMT客户端已启动**: 运行脚本前需要先启动QMT客户端并登录
2. **网络连接**: 下载数据需要稳定的网络连接
3. **数据存储**: 数据会下载到QMT的本地数据目录
4. **股票列表**: 如果不指定股票列表，脚本会尝试从指数获取成分股，如果失败则使用示例列表

## 使用因子计算模块

下载完数据后，可以使用 `factors/eod_factors.py` 中的因子计算模块：

```python
from factors.eod_factors import EODFactorCalculator, calculate_all_factors
import pandas as pd

# 创建计算器
calculator = EODFactorCalculator()

# 加载数据（需要从QMT读取）
daily_data = ...  # 日线数据
bar_data = {
    '1m': ...,   # 1分钟数据
    '5m': ...,   # 5分钟数据
    '60m': ...   # 60分钟数据
}

# 计算因子
factors = calculate_all_factors(daily_data, bar_data, cur_date=20250115)
```

## 常见问题

1. **ImportError: No module named 'xtquant'**
   - 确保已安装xtquant库
   - 确保QMT客户端已正确安装

2. **下载失败**
   - 检查QMT客户端是否正常运行
   - 检查网络连接
   - 检查股票代码格式是否正确（如：600000.SH）

3. **数据不完整**
   - 某些股票可能没有所有周期的数据
   - 新上市股票可能缺少历史数据

