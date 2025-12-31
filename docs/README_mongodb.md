# MongoDB数据存储说明

## 概述

所有数据现在都使用MongoDB存储，不再使用文件系统（E:/StockData）。

## 配置

MongoDB配置已在全局配置中设置，通过 `vnpy/trader/setting.py` 或 `vt_setting.json` 文件配置：

```json
{
    "database.name": "mongodb",
    "database.database": "vnpy",
    "database.host": "localhost",
    "database.port": 27017,
    "database.user": "",
    "database.password": ""
}
```

## 数据存储

### K线数据

所有K线数据（日线、1分钟、5分钟、60分钟）都存储在MongoDB中：

- **集合命名**: `dbbardata_{symbol}_{exchange}_{interval}`
- **示例**: 
  - `dbbardata_300001_SZSE_1m` (300001.SZ的1分钟K线)
  - `dbbardata_600000_SSE_1d` (600000.SH的日线)

### 数据格式

每个文档包含：
- `symbol`: 股票代码
- `exchange`: 交易所
- `datetime`: 时间
- `interval`: 周期
- `open_price`, `high_price`, `low_price`, `close_price`: 价格
- `volume`: 成交量
- `turnover`: 成交额

## 使用方式

### 下载数据

```bash
# 下载创业板所有股票数据（自动保存到MongoDB）
python scripts/download_eod_data_2025.py
```

### 从数据库读取数据

```python
from datetime import datetime
from vnpy.trader.database import get_database, DB_TZ
from vnpy.trader.constant import Exchange, Interval

# 获取数据库实例（使用全局配置）
database = get_database()

# 读取数据
bars = database.load_bar_data(
    symbol="300001",
    exchange=Exchange.SZSE,
    interval=Interval.MINUTE,
    start=datetime(2025, 1, 1, tzinfo=DB_TZ),
    end=datetime(2025, 1, 31, tzinfo=DB_TZ)
)
```

## 注意事项

1. **确保MongoDB运行**: 下载前确保MongoDB服务已启动
2. **全局配置**: 数据库配置通过vnpy的全局配置管理，不需要在代码中硬编码
3. **数据去重**: vnpy的MongoDB驱动会自动处理数据去重
4. **性能**: 使用`stream=True`参数提高批量写入性能

