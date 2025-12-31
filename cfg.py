"""
配置模块 - 用于eod_sig.py等因子计算脚本
所有数据路径统一指向 E:\StockData
"""
import os
from pathlib import Path

# 统一的数据根目录
STOCK_DATA_ROOT = Path("E:/StockData")

# 确保目录存在
STOCK_DATA_ROOT.mkdir(parents=True, exist_ok=True)

# 兼容eod_sig.py中使用的路径配置
class Config:
    """配置类"""
    
    # 数据根目录
    param_path_root = str(STOCK_DATA_ROOT)
    
    # 分钟数据文件路径
    param_path_src_minbar_file = str(STOCK_DATA_ROOT / "minute")
    
    # 指数分钟数据文件路径
    param_path_src_index_minbar_file = str(STOCK_DATA_ROOT / "index" / "minute")
    
    # 原始逐笔数据路径
    param_path_src_ori_file = str(STOCK_DATA_ROOT / "trans")
    
    # 调试输出目录
    debug_output_dir = str(STOCK_DATA_ROOT / "debug")
    
    # 确保所有目录存在
    def __init__(self):
        dirs = [
            self.param_path_root,
            self.param_path_src_minbar_file,
            self.param_path_src_index_minbar_file,
            self.param_path_src_ori_file,
            self.debug_output_dir,
        ]
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)

# 创建全局配置实例
cfg = Config()

