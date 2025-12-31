"""
MongoDB存储辅助模块
用于存储非K线数据（如成分股、数据集、模型、信号等）
"""
import json
import pickle
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    from bson.binary import Binary
    from bson.objectid import ObjectId
except ImportError:
    MongoClient = None
    logger = logging.getLogger(__name__)
    logger.warning("pymongo未安装，MongoDB存储功能不可用")

from .setting import SETTINGS

logger = logging.getLogger(__name__)


class MongoDBStorage:
    """MongoDB存储辅助类"""
    
    def __init__(self, collection_prefix: str = "vnpy_"):
        """
        初始化MongoDB存储
        Args:
            collection_prefix: 集合名称前缀
        """
        if MongoClient is None:
            raise ImportError("请安装pymongo: pip install pymongo")
        
        self.collection_prefix = collection_prefix
        self.client = None
        self.db = None
        
        # 从配置获取连接信息
        host = SETTINGS.get("database.host", "localhost")
        port = SETTINGS.get("database.port", 27017)
        database_name = SETTINGS.get("database.database", "vnpy")
        username = SETTINGS.get("database.user", "")
        password = SETTINGS.get("database.password", "")
        
        try:
            # 构建连接字符串
            if username and password:
                uri = f"mongodb://{username}:{password}@{host}:{port}/{database_name}"
            else:
                uri = f"mongodb://{host}:{port}/{database_name}"
            
            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[database_name]
            
            # 测试连接
            self.client.admin.command('ping')
            logger.info(f"MongoDB存储连接成功: {host}:{port}/{database_name}")
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"MongoDB连接失败: {e}")
            raise
    
    def get_collection(self, name: str):
        """获取集合"""
        collection_name = f"{self.collection_prefix}{name}"
        return self.db[collection_name]
    
    def save_component_data(self, index_symbol: str, 
                           index_components: Dict[str, List[str]]) -> bool:
        """
        保存指数成分股数据
        Args:
            index_symbol: 指数代码
            index_components: 成分股字典 {date_str: [symbols]}
        Returns:
            是否成功
        """
        try:
            collection = self.get_collection("components")
            
            # 转换为文档格式
            for date_str, symbols in index_components.items():
                collection.update_one(
                    {"index_symbol": index_symbol, "date": date_str},
                    {
                        "$set": {
                            "index_symbol": index_symbol,
                            "date": date_str,
                            "symbols": symbols,
                            "updated_at": datetime.now()
                        }
                    },
                    upsert=True
                )
            
            return True
        except Exception as e:
            logger.error(f"保存成分股数据失败: {e}")
            return False
    
    def load_component_data(self, index_symbol: str, 
                           start: datetime, end: datetime) -> Dict[datetime, List[str]]:
        """
        加载指数成分股数据
        Args:
            index_symbol: 指数代码
            start: 开始日期
            end: 结束日期
        Returns:
            成分股字典 {datetime: [symbols]}
        """
        try:
            collection = self.get_collection("components")
            
            # 查询数据
            query = {
                "index_symbol": index_symbol,
                "date": {
                    "$gte": start.strftime("%Y-%m-%d"),
                    "$lte": end.strftime("%Y-%m-%d")
                }
            }
            
            results = collection.find(query).sort("date", 1)
            
            index_components = {}
            for doc in results:
                date_str = doc["date"]
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                index_components[dt] = doc["symbols"]
            
            return index_components
        except Exception as e:
            logger.error(f"加载成分股数据失败: {e}")
            return {}
    
    def save_dataset(self, name: str, dataset: Any) -> bool:
        """
        保存数据集（使用pickle序列化）
        Args:
            name: 数据集名称
            dataset: 数据集对象
        Returns:
            是否成功
        """
        try:
            collection = self.get_collection("datasets")
            
            # 序列化数据
            data_binary = Binary(pickle.dumps(dataset))
            
            collection.update_one(
                {"name": name},
                {
                    "$set": {
                        "name": name,
                        "data": data_binary,
                        "updated_at": datetime.now()
                    }
                },
                upsert=True
            )
            
            return True
        except Exception as e:
            logger.error(f"保存数据集失败: {e}")
            return False
    
    def load_dataset(self, name: str) -> Optional[Any]:
        """
        加载数据集
        Args:
            name: 数据集名称
        Returns:
            数据集对象或None
        """
        try:
            collection = self.get_collection("datasets")
            
            doc = collection.find_one({"name": name})
            if doc and "data" in doc:
                return pickle.loads(doc["data"])
            
            return None
        except Exception as e:
            logger.error(f"加载数据集失败: {e}")
            return None
    
    def save_model(self, name: str, model: Any) -> bool:
        """
        保存模型（使用pickle序列化）
        Args:
            name: 模型名称
            model: 模型对象
        Returns:
            是否成功
        """
        try:
            collection = self.get_collection("models")
            
            # 序列化数据
            data_binary = Binary(pickle.dumps(model))
            
            collection.update_one(
                {"name": name},
                {
                    "$set": {
                        "name": name,
                        "data": data_binary,
                        "updated_at": datetime.now()
                    }
                },
                upsert=True
            )
            
            return True
        except Exception as e:
            logger.error(f"保存模型失败: {e}")
            return False
    
    def load_model(self, name: str) -> Optional[Any]:
        """
        加载模型
        Args:
            name: 模型名称
        Returns:
            模型对象或None
        """
        try:
            collection = self.get_collection("models")
            
            doc = collection.find_one({"name": name})
            if doc and "data" in doc:
                return pickle.loads(doc["data"])
            
            return None
        except Exception as e:
            logger.error(f"加载模型失败: {e}")
            return None
    
    def save_signal(self, name: str, signal_data: Dict[str, Any]) -> bool:
        """
        保存信号数据
        Args:
            name: 信号名称
            signal_data: 信号数据（字典格式）
        Returns:
            是否成功
        """
        try:
            collection = self.get_collection("signals")
            
            # 转换datetime为字符串
            signal_doc = {}
            for key, value in signal_data.items():
                if isinstance(value, datetime):
                    signal_doc[key] = value.isoformat()
                else:
                    signal_doc[key] = value
            
            collection.update_one(
                {"name": name},
                {
                    "$set": {
                        "name": name,
                        "data": signal_doc,
                        "updated_at": datetime.now()
                    }
                },
                upsert=True
            )
            
            return True
        except Exception as e:
            logger.error(f"保存信号失败: {e}")
            return False
    
    def load_signal(self, name: str) -> Optional[Dict[str, Any]]:
        """
        加载信号数据
        Args:
            name: 信号名称
        Returns:
            信号数据字典或None
        """
        try:
            collection = self.get_collection("signals")
            
            doc = collection.find_one({"name": name})
            if doc and "data" in doc:
                return doc["data"]
            
            return None
        except Exception as e:
            logger.error(f"加载信号失败: {e}")
            return None
    
    def list_datasets(self) -> List[str]:
        """列出所有数据集名称"""
        try:
            collection = self.get_collection("datasets")
            return [doc["name"] for doc in collection.find({}, {"name": 1})]
        except Exception as e:
            logger.error(f"列出数据集失败: {e}")
            return []
    
    def list_models(self) -> List[str]:
        """列出所有模型名称"""
        try:
            collection = self.get_collection("models")
            return [doc["name"] for doc in collection.find({}, {"name": 1})]
        except Exception as e:
            logger.error(f"列出模型失败: {e}")
            return []
    
    def list_signals(self) -> List[str]:
        """列出所有信号名称"""
        try:
            collection = self.get_collection("signals")
            return [doc["name"] for doc in collection.find({}, {"name": 1})]
        except Exception as e:
            logger.error(f"列出信号失败: {e}")
            return []
    
    def delete_dataset(self, name: str) -> bool:
        """删除数据集"""
        try:
            collection = self.get_collection("datasets")
            result = collection.delete_one({"name": name})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除数据集失败: {e}")
            return False
    
    def delete_model(self, name: str) -> bool:
        """删除模型"""
        try:
            collection = self.get_collection("models")
            result = collection.delete_one({"name": name})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除模型失败: {e}")
            return False
    
    def delete_signal(self, name: str) -> bool:
        """删除信号"""
        try:
            collection = self.get_collection("signals")
            result = collection.delete_one({"name": name})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除信号失败: {e}")
            return False
    
    def close(self):
        """关闭连接"""
        if self.client:
            self.client.close()

