"""
统一的数据路径配置模块
所有数据存储路径都指向 E:\StockData
"""
import os
from pathlib import Path

# 统一的数据根目录
STOCK_DATA_ROOT = Path("E:/StockData")

# 确保目录存在
STOCK_DATA_ROOT.mkdir(parents=True, exist_ok=True)

# 各种数据子目录
DATA_PATHS = {
    # 基础数据目录
    "root": STOCK_DATA_ROOT,
    
    # 日线数据
    "daily": STOCK_DATA_ROOT / "daily",
    
    # 分钟数据
    "minute_1m": STOCK_DATA_ROOT / "minute" / "1m",
    "minute_5m": STOCK_DATA_ROOT / "minute" / "5m",
    "minute_60m": STOCK_DATA_ROOT / "minute" / "60m",
    
    # 因子数据
    "factors": STOCK_DATA_ROOT / "factors",
    
    # 输出数据
    "output": STOCK_DATA_ROOT / "output",
    
    # 日志
    "log": STOCK_DATA_ROOT / "log",
    
    # 临时文件
    "temp": STOCK_DATA_ROOT / "temp",
    
    # Alpha Lab数据
    "alpha_lab": STOCK_DATA_ROOT / "alpha_lab",
    
    # 数据库
    "database": STOCK_DATA_ROOT / "database",
}

# 创建所有必要的目录
for path in DATA_PATHS.values():
    if isinstance(path, Path):
        path.mkdir(parents=True, exist_ok=True)


def get_data_path(key: str) -> Path:
    """
    获取数据路径
    Args:
        key: 路径键名
    Returns:
        Path对象
    """
    return DATA_PATHS.get(key, STOCK_DATA_ROOT / key)


def get_stock_data_root() -> Path:
    """获取股票数据根目录"""
    return STOCK_DATA_ROOT

