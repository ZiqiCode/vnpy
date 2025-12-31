# MongoDB配置说明

## 概述

vnpy工程已配置为使用MongoDB存储数据。所有下载的K线数据将自动保存到MongoDB数据库中。

## 配置信息

### 默认配置（已设置）

在 `vnpy/trader/setting.py` 中已配置：

```python
"database.name": "mongodb",
"database.database": "vnpy",
"database.host": "localhost",
"database.port": 27017,
"database.user": "",
"database.password": ""
```

### 配置字段说明

| 字段 | 含义 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| database.name | 数据库类型 | 是 | mongodb | 固定为"mongodb" |
| database.host | 服务器地址 | 是 | localhost | MongoDB服务器IP或域名 |
| database.port | 端口号 | 是 | 27017 | MongoDB默认端口 |
| database.database | 数据库实例 | 是 | vnpy | 数据库名称 |
| database.user | 用户名 | 否 | "" | 如果MongoDB启用了认证，填写用户名 |
| database.password | 密码 | 否 | "" | 如果MongoDB启用了认证，填写密码 |

## 修改配置

### 方法1：修改代码（推荐用于开发环境）

直接修改 `vnpy/trader/setting.py` 文件中的配置值。

### 方法2：使用配置文件（推荐用于生产环境）

在项目根目录创建或修改 `vt_setting.json` 文件：

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

配置文件会覆盖代码中的默认配置。

## MongoDB安装和启动

### Windows安装

1. 下载MongoDB Community Server: https://www.mongodb.com/try/download/community
2. 安装并启动MongoDB服务
3. 默认端口为27017

### Linux安装

```bash
# Ubuntu/Debian
sudo apt-get install mongodb

# CentOS/RHEL
sudo yum install mongodb

# 启动服务
sudo systemctl start mongod
sudo systemctl enable mongod
```

### Docker安装（推荐）

```bash
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -v mongodb_data:/data/db \
  mongo:latest
```

## 验证配置

### 1. 检查MongoDB是否运行

```bash
# Windows
net start MongoDB

# Linux
sudo systemctl status mongod

# 或使用mongo客户端连接
mongo --host localhost --port 27017
```

### 2. 测试数据库连接

运行下载脚本，如果看到以下日志，说明连接成功：

```
数据库连接成功（MongoDB）
```

如果连接失败，会显示错误信息，并只下载到QMT本地，不保存到数据库。

## 数据存储结构

### 集合（Collection）命名

MongoDB中，数据按以下规则存储：

- **K线数据**: `dbbardata_{symbol}_{exchange}_{interval}`
  - 例如：`dbbardata_600000_SSE_1m`（600000.SH的1分钟K线）
  - 例如：`dbbardata_000001_SZSE_1d`（000001.SZ的日线）

### 数据格式

每个文档（Document）包含以下字段：

```json
{
    "_id": ObjectId("..."),
    "symbol": "600000",
    "exchange": "SSE",
    "datetime": ISODate("2025-01-01T09:30:00Z"),
    "interval": "1m",
    "volume": 1234567.0,
    "turnover": 12345678.9,
    "open_price": 10.5,
    "high_price": 10.6,
    "low_price": 10.4,
    "close_price": 10.55,
    "open_interest": 0.0
}
```

## 使用示例

### 从数据库读取数据

```python
from datetime import datetime
from vnpy.trader.database import get_database, DB_TZ
from vnpy.trader.constant import Exchange, Interval

# 获取数据库实例
database = get_database()

# 读取数据
bars = database.load_bar_data(
    symbol="600000",
    exchange=Exchange.SSE,
    interval=Interval.MINUTE,
    start=datetime(2025, 1, 1, tzinfo=DB_TZ),
    end=datetime(2025, 1, 31, tzinfo=DB_TZ)
)

print(f"读取到 {len(bars)} 条K线数据")
```

### 查看数据库中的数据

使用MongoDB客户端工具（如MongoDB Compass）或命令行：

```javascript
// 连接到数据库
use vnpy

// 查看所有集合
show collections

// 查看某个集合的数据
db.dbbardata_600000_SSE_1m.find().limit(10)

// 统计数据量
db.dbbardata_600000_SSE_1m.countDocuments()
```

## 注意事项

1. **确保MongoDB服务运行**: 下载脚本会自动尝试连接MongoDB，如果连接失败，数据仍会下载到QMT本地。

2. **数据去重**: vnpy的MongoDB驱动会自动处理数据去重，重复保存不会产生重复数据。

3. **性能优化**: 
   - 对于大量数据，建议使用`stream=True`参数（已自动启用）
   - 可以创建索引提高查询速度

4. **备份**: 定期备份MongoDB数据，可以使用`mongodump`命令。

5. **认证**: 如果MongoDB启用了认证，需要在配置中填写用户名和密码。

## 故障排查

### 问题1：连接失败

**错误信息**: `数据库连接失败: ...`

**解决方案**:
- 检查MongoDB服务是否运行
- 检查host和port配置是否正确
- 检查防火墙设置
- 如果启用了认证，检查用户名密码

### 问题2：数据未保存

**现象**: 下载成功但数据库中没有数据

**解决方案**:
- 检查日志中是否有"保存到数据库"的提示
- 检查MongoDB日志
- 确认数据库连接成功

### 问题3：数据格式错误

**现象**: 保存时报错

**解决方案**:
- 检查xtquant返回的数据格式
- 查看详细错误日志
- 可能需要调整数据转换逻辑

## 相关文件

- `vnpy/trader/setting.py`: 数据库配置
- `vnpy/trader/database.py`: 数据库接口
- `scripts/download_eod_data_2025.py`: 下载脚本（已集成MongoDB保存功能）

