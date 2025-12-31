# 数据路径配置说明

## 概述

整个vnpy工程的所有数据下载和存储路径已统一配置为 **E:\StockData**

## 配置位置

### 1. 核心配置模块

- **`vnpy/trader/data_path.py`**: 统一的数据路径配置模块
  - 定义了 `STOCK_DATA_ROOT = Path("E:/StockData")`
  - 提供了各种数据子目录的路径映射

- **`cfg.py`**: 兼容eod_sig.py等因子计算脚本的配置模块
  - 提供了 `cfg.param_path_root` 等配置项
  - 所有路径都指向 E:\StockData

### 2. 修改的文件

#### vnpy/trader/utility.py
- 修改了 `TRADER_DIR` 和 `TEMP_DIR` 的获取逻辑
- 现在使用 E:\StockData 作为交易数据目录

#### scripts/download_eod_data_2025.py
- 添加了数据路径说明
- 注意：QMT下载的数据会保存到QMT本地数据目录，但脚本会提示数据存储位置

## 目录结构

```
E:\StockData\
├── daily\              # 日线数据
├── minute\             # 分钟数据
│   ├── 1m\            # 1分钟数据
│   ├── 5m\            # 5分钟数据
│   └── 60m\           # 60分钟数据
├── factors\            # 因子数据
├── output\             # 输出数据
├── log\                # 日志文件
├── temp\               # 临时文件
├── alpha_lab\          # Alpha Lab数据
├── database\           # 数据库文件
├── index\              # 指数数据
│   └── minute\        # 指数分钟数据
├── trans\              # 逐笔数据
└── debug\              # 调试输出
```

## 使用方法

### 1. 在代码中使用统一路径

```python
from vnpy.trader.data_path import get_data_path, STOCK_DATA_ROOT

# 获取数据根目录
data_root = STOCK_DATA_ROOT

# 获取特定数据路径
daily_path = get_data_path("daily")
minute_1m_path = get_data_path("minute_1m")
```

### 2. 在因子计算脚本中使用

```python
import cfg

# 使用配置的路径
daily_folder = cfg.param_path_root  # E:\StockData
bar_folder = cfg.param_path_src_minbar_file  # E:\StockData\minute
```

### 3. AlphaLab使用

```python
from vnpy.alpha import AlphaLab
from vnpy.trader.data_path import get_data_path

# 使用统一路径创建AlphaLab
lab = AlphaLab(str(get_data_path("alpha_lab")))
```

## 注意事项

1. **QMT数据**: QMT下载的历史数据会保存到QMT的本地数据目录（由QMT客户端管理），不是E:\StockData。但因子计算结果等其他数据会保存到E:\StockData。

2. **目录自动创建**: 所有必要的目录会在首次使用时自动创建。

3. **路径格式**: 使用Path对象，支持跨平台（Windows/Linux/Mac），但在Windows上会使用E:\StockData。

4. **权限**: 确保对E:\StockData目录有读写权限。

## 修改路径

如果需要修改数据存储路径，请修改以下文件：

1. **`vnpy/trader/data_path.py`**: 修改 `STOCK_DATA_ROOT`
2. **`cfg.py`**: 修改 `STOCK_DATA_ROOT`

修改后，所有使用这些配置的代码都会自动使用新路径。

