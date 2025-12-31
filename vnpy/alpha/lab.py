import json
import shelve
import pickle
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from functools import lru_cache

import polars as pl

from vnpy.trader.object import BarData
from vnpy.trader.constant import Interval
from vnpy.trader.utility import extract_vt_symbol
from vnpy.trader.database import get_database, DB_TZ

from .logger import logger
from .dataset import AlphaDataset, to_datetime
from .model import AlphaModel

# 尝试导入MongoDB存储
try:
    from vnpy.trader.mongodb_storage import MongoDBStorage
    MONGODB_STORAGE_AVAILABLE = True
except ImportError:
    MONGODB_STORAGE_AVAILABLE = False
    MongoDBStorage = None


class AlphaLab:
    """Alpha Research Laboratory"""

    def __init__(self, lab_path: str = None, use_mongodb: bool = True) -> None:
        """
        Constructor
        Args:
            lab_path: 实验室路径（用于存储非K线数据，如dataset、model等）
            use_mongodb: 是否使用MongoDB存储K线数据（默认True）
        """
        # 初始化数据库（用于K线数据）
        self.use_mongodb = use_mongodb
        if self.use_mongodb:
            try:
                self.database = get_database()
                logger.info("AlphaLab: 使用MongoDB存储K线数据")
            except Exception as e:
                logger.warning(f"AlphaLab: MongoDB连接失败，将使用文件存储: {e}")
                self.use_mongodb = False
                self.database = None
        else:
            self.database = None
        
        # 初始化MongoDB存储（用于其他数据）
        self.mongodb_storage = None
        if MONGODB_STORAGE_AVAILABLE and self.use_mongodb:
            try:
                self.mongodb_storage = MongoDBStorage()
                logger.info("AlphaLab: 使用MongoDB存储其他数据（成分股、数据集、模型等）")
            except Exception as e:
                logger.warning(f"AlphaLab: MongoDB存储初始化失败: {e}")
                self.mongodb_storage = None
        
        # Set data paths（用于存储非K线数据，如dataset、model等）
        if lab_path:
            self.lab_path: Path = Path(lab_path)
        else:
            # 使用当前工作目录下的默认路径
            cwd: Path = Path.cwd()
            self.lab_path: Path = cwd.joinpath(".vntrader", "alpha_lab")

        self.daily_path: Path = self.lab_path.joinpath("daily")
        self.minute_path: Path = self.lab_path.joinpath("minute")
        self.component_path: Path = self.lab_path.joinpath("component")

        self.dataset_path: Path = self.lab_path.joinpath("dataset")
        self.model_path: Path = self.lab_path.joinpath("model")
        self.signal_path: Path = self.lab_path.joinpath("signal")

        self.contract_path: Path = self.lab_path.joinpath("contract.json")

        # Create folders（仅用于非K线数据）
        for path in [
            self.lab_path,
            self.component_path,
            self.dataset_path,
            self.model_path,
            self.signal_path
        ]:
            if not path.exists():
                path.mkdir(parents=True)

    def save_bar_data(self, bars: list[BarData]) -> None:
        """Save bar data to MongoDB"""
        if not bars:
            return

        # 优先使用MongoDB
        if self.use_mongodb and self.database:
            try:
                self.database.save_bar_data(bars, stream=True)
                logger.debug(f"保存 {len(bars)} 条K线数据到MongoDB")
                return
            except Exception as e:
                logger.warning(f"MongoDB保存失败，回退到文件存储: {e}")
                self.use_mongodb = False
        
        # 回退到文件存储（兼容旧代码）
        bar: BarData = bars[0]

        if bar.interval == Interval.DAILY:
            file_path: Path = self.daily_path.joinpath(f"{bar.vt_symbol}.parquet")
        elif bar.interval == Interval.MINUTE:
            file_path = self.minute_path.joinpath(f"{bar.vt_symbol}.parquet")
        elif bar.interval:
            logger.error(f"Unsupported interval {bar.interval.value}")
            return

        # 确保目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)

        data: list = []
        for bar in bars:
            bar_data: dict = {
                "datetime": bar.datetime.replace(tzinfo=None),
                "open": bar.open_price,
                "high": bar.high_price,
                "low": bar.low_price,
                "close": bar.close_price,
                "volume": bar.volume,
                "turnover": bar.turnover,
                "open_interest": bar.open_interest
            }
            data.append(bar_data)

        new_df: pl.DataFrame = pl.DataFrame(data)

        # If file exists, read and merge
        if file_path.exists():
            old_df: pl.DataFrame = pl.read_parquet(file_path)

            new_df = pl.concat([old_df, new_df])

            new_df = new_df.unique(subset=["datetime"])

            new_df = new_df.sort("datetime")

        # Save to file
        new_df.write_parquet(file_path)

    def load_bar_data(
        self,
        vt_symbol: str,
        interval: Interval | str,
        start: datetime | str,
        end: datetime | str
    ) -> list[BarData]:
        """Load bar data from MongoDB or file"""
        # Convert types
        if isinstance(interval, str):
            interval = Interval(interval)

        start = to_datetime(start)
        end = to_datetime(end)

        # 优先从MongoDB加载
        if self.use_mongodb and self.database:
            try:
                symbol, exchange = extract_vt_symbol(vt_symbol)
                bars = self.database.load_bar_data(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    start=start.replace(tzinfo=DB_TZ),
                    end=end.replace(tzinfo=DB_TZ)
                )
                if bars:
                    logger.debug(f"从MongoDB加载 {len(bars)} 条K线数据")
                    return bars
            except Exception as e:
                logger.warning(f"从MongoDB加载失败，尝试从文件加载: {e}")

        # 回退到文件存储（兼容旧代码）
        # Get folder path
        if interval == Interval.DAILY:
            folder_path: Path = self.daily_path
        elif interval == Interval.MINUTE:
            folder_path = self.minute_path
        else:
            logger.error(f"Unsupported interval {interval.value}")
            return []

        # Check if file exists
        file_path: Path = folder_path.joinpath(f"{vt_symbol}.parquet")
        if not file_path.exists():
            logger.warning(f"File {file_path} does not exist")
            return []

        # Open file
        df: pl.DataFrame = pl.read_parquet(file_path)

        # Filter by date range
        df = df.filter((pl.col("datetime") >= start) & (pl.col("datetime") <= end))

        # Convert to BarData objects
        bars: list[BarData] = []

        symbol, exchange = extract_vt_symbol(vt_symbol)

        for row in df.iter_rows(named=True):
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=row["datetime"],
                interval=interval,
                open_price=row["open"],
                high_price=row["high"],
                low_price=row["low"],
                close_price=row["close"],
                volume=row["volume"],
                turnover=row["turnover"],
                open_interest=row["open_interest"],
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_bar_df(
        self,
        vt_symbols: list[str],
        interval: Interval | str,
        start: datetime | str,
        end: datetime | str,
        extended_days: int
    ) -> pl.DataFrame | None:
        """Load bar data as DataFrame from MongoDB or file"""
        if not vt_symbols:
            return None

        # Convert types
        if isinstance(interval, str):
            interval = Interval(interval)

        start = to_datetime(start) - timedelta(days=extended_days)
        end = to_datetime(end) + timedelta(days=extended_days // 10)

        # 优先从MongoDB加载
        if self.use_mongodb and self.database:
            try:
                all_bars = []
                for vt_symbol in vt_symbols:
                    symbol, exchange = extract_vt_symbol(vt_symbol)
                    bars = self.database.load_bar_data(
                        symbol=symbol,
                        exchange=exchange,
                        interval=interval,
                        start=start.replace(tzinfo=DB_TZ),
                        end=end.replace(tzinfo=DB_TZ)
                    )
                    if bars:
                        all_bars.extend(bars)
                
                if all_bars:
                    # 转换为DataFrame
                    data = []
                    for bar in all_bars:
                        bar_data = {
                            "datetime": bar.datetime.replace(tzinfo=None),
                            "open": bar.open_price,
                            "high": bar.high_price,
                            "low": bar.low_price,
                            "close": bar.close_price,
                            "volume": bar.volume,
                            "turnover": bar.turnover,
                            "open_interest": bar.open_interest,
                            "vt_symbol": bar.vt_symbol
                        }
                        data.append(bar_data)
                    
                    df = pl.DataFrame(data)
                    # 继续后续处理
                    return self._process_bar_df(df)
            except Exception as e:
                logger.warning(f"从MongoDB加载DataFrame失败，尝试从文件加载: {e}")

        # 回退到文件存储
        # Get folder path
        if interval == Interval.DAILY:
            folder_path: Path = self.daily_path
        elif interval == Interval.MINUTE:
            folder_path = self.minute_path
        else:
            logger.error(f"Unsupported interval {interval.value}")
            return None

        # Read data for each symbol
        dfs: list = []

        for vt_symbol in vt_symbols:
            # Check if file exists
            file_path: Path = folder_path.joinpath(f"{vt_symbol}.parquet")
            if not file_path.exists():
                logger.warning(f"File {file_path} does not exist")
                continue

            # Open file
            df: pl.DataFrame = pl.read_parquet(file_path)

            # Filter by date range
            df = df.filter((pl.col("datetime") >= start) & (pl.col("datetime") <= end))

            # Check for empty data
            if df.is_empty():
                continue

            # Add symbol column if not exists
            if "vt_symbol" not in df.columns:
                df = df.with_columns(pl.lit(vt_symbol).alias("vt_symbol"))

            # Process DataFrame
            df = self._process_bar_df(df)

            # Cache in list
            dfs.append(df)

        if not dfs:
            return None

        # Concatenate results
        result_df: pl.DataFrame = pl.concat(dfs)
        return result_df
    
    def _process_bar_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process bar DataFrame (normalize prices, add vwap, etc.)"""
        if df.is_empty():
            return df
        
        # Specify data types and add vwap
        df = df.with_columns(
            (pl.col("turnover") / pl.col("volume")).alias("vwap")
        )

        # Normalize prices by first close price
        close_0: float = df.select(pl.col("close")).item(0, 0)
        if close_0 > 0:
            df = df.with_columns(
                (pl.col("open") / close_0).alias("open"),
                (pl.col("high") / close_0).alias("high"),
                (pl.col("low") / close_0).alias("low"),
                (pl.col("close") / close_0).alias("close"),
            )

        # Convert zeros to NaN for suspended trading days
        numeric_columns: list = [col for col in df.columns 
                                 if col not in ["datetime", "vt_symbol"]]
        
        if numeric_columns:
            mask: pl.Series = df[numeric_columns].sum_horizontal() == 0
            
            df = df.with_columns(
                [pl.when(mask).then(float("nan")).otherwise(pl.col(col)).alias(col) 
                 for col in numeric_columns]
            )

        return df

    def save_component_data(
        self,
        index_symbol: str,
        index_components: dict[str, list[str]]
    ) -> None:
        """Save index component data to MongoDB or file"""
        # 优先使用MongoDB
        if self.mongodb_storage:
            try:
                # 转换格式：datetime -> str
                components_dict = {}
                for dt, symbols in index_components.items():
                    if isinstance(dt, datetime):
                        date_str = dt.strftime("%Y-%m-%d")
                    else:
                        date_str = str(dt)
                    components_dict[date_str] = symbols
                
                if self.mongodb_storage.save_component_data(index_symbol, components_dict):
                    logger.debug(f"成分股数据已保存到MongoDB: {index_symbol}")
                    return
            except Exception as e:
                logger.warning(f"MongoDB保存成分股数据失败，回退到文件存储: {e}")
        
        # 回退到文件存储
        file_path: Path = self.component_path.joinpath(f"{index_symbol}")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with shelve.open(str(file_path)) as db:
            # 转换datetime为字符串key
            for dt, symbols in index_components.items():
                if isinstance(dt, datetime):
                    key = dt.strftime("%Y-%m-%d")
                else:
                    key = str(dt)
                db[key] = symbols

    @lru_cache      # noqa
    def load_component_data(
        self,
        index_symbol: str,
        start: datetime | str,
        end: datetime | str
    ) -> dict[datetime, list[str]]:
        """Load index component data from MongoDB or file"""
        start = to_datetime(start)
        end = to_datetime(end)
        
        # 优先从MongoDB加载
        if self.mongodb_storage:
            try:
                components = self.mongodb_storage.load_component_data(index_symbol, start, end)
                if components:
                    logger.debug(f"从MongoDB加载成分股数据: {index_symbol}")
                    return components
            except Exception as e:
                logger.warning(f"从MongoDB加载成分股数据失败，尝试从文件加载: {e}")
        
        # 回退到文件存储
        file_path: Path = self.component_path.joinpath(f"{index_symbol}")
        
        if not file_path.exists():
            return {}

        with shelve.open(str(file_path)) as db:
            keys: list[str] = list(db.keys())
            keys.sort()

            index_components: dict[datetime, list[str]] = {}
            for key in keys:
                dt: datetime = datetime.strptime(key, "%Y-%m-%d")
                if start <= dt <= end:
                    index_components[dt] = db[key]

            return index_components

    def load_component_symbols(
        self,
        index_symbol: str,
        start: datetime | str,
        end: datetime | str
    ) -> list[str]:
        """Collect index component symbols"""
        index_components: dict[datetime, list[str]] = self.load_component_data(
            index_symbol,
            start,
            end
        )

        component_symbols: set[str] = set()

        for vt_symbols in index_components.values():
            component_symbols.update(vt_symbols)

        return list(component_symbols)

    def load_component_filters(
        self,
        index_symbol: str,
        start: datetime | str,
        end: datetime | str
    ) -> dict[str, list[tuple[datetime, datetime]]]:
        """Collect index component duration filters"""
        index_components: dict[datetime, list[str]] = self.load_component_data(
            index_symbol,
            start,
            end
        )

        # Get all trading dates and sort
        trading_dates: list[datetime] = sorted(index_components.keys())

        # Initialize component duration dictionary
        component_filters: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)

        # Get all component symbols
        all_symbols: set[str] = set()
        for vt_symbols in index_components.values():
            all_symbols.update(vt_symbols)

        # Iterate through each component to identify its duration in the index
        for vt_symbol in all_symbols:
            period_start: datetime | None = None
            period_end: datetime | None = None

            # Iterate through each trading day to identify continuous holding periods
            for trading_date in trading_dates:
                if vt_symbol in index_components[trading_date]:
                    if period_start is None:
                        period_start = trading_date

                    period_end = trading_date
                else:
                    if period_start and period_end:
                        component_filters[vt_symbol].append((period_start, period_end))
                        period_start = None
                        period_end = None

            # Handle the last holding period
            if period_start and period_end:
                component_filters[vt_symbol].append((period_start, period_end))

        return component_filters

    def add_contract_setting(
        self,
        vt_symbol: str,
        long_rate: float,
        short_rate: float,
        size: float,
        pricetick: float
    ) -> None:
        """Add contract information"""
        contracts: dict = {}

        if self.contract_path.exists():
            with open(self.contract_path, encoding="UTF-8") as f:
                contracts = json.load(f)

        contracts[vt_symbol] = {
            "long_rate": long_rate,
            "short_rate": short_rate,
            "size": size,
            "pricetick": pricetick
        }

        with open(self.contract_path, mode="w+", encoding="UTF-8") as f:
            json.dump(
                contracts,
                f,
                indent=4,
                ensure_ascii=False
            )

    def load_contract_setttings(self) -> dict:
        """Load contract settings"""
        contracts: dict = {}

        if self.contract_path.exists():
            with open(self.contract_path, encoding="UTF-8") as f:
                contracts = json.load(f)

        return contracts

    def save_dataset(self, name: str, dataset: AlphaDataset) -> None:
        """Save dataset to MongoDB or file"""
        # 优先使用MongoDB
        if self.mongodb_storage:
            try:
                if self.mongodb_storage.save_dataset(name, dataset):
                    logger.debug(f"数据集已保存到MongoDB: {name}")
                    return
            except Exception as e:
                logger.warning(f"MongoDB保存数据集失败，回退到文件存储: {e}")
        
        # 回退到文件存储
        file_path: Path = self.dataset_path.joinpath(f"{name}.pkl")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, mode="wb") as f:
            pickle.dump(dataset, f)

    def load_dataset(self, name: str) -> AlphaDataset | None:
        """Load dataset from MongoDB or file"""
        # 优先从MongoDB加载
        if self.mongodb_storage:
            try:
                dataset = self.mongodb_storage.load_dataset(name)
                if dataset:
                    logger.debug(f"从MongoDB加载数据集: {name}")
                    return dataset
            except Exception as e:
                logger.warning(f"从MongoDB加载数据集失败，尝试从文件加载: {e}")
        
        # 回退到文件存储
        file_path: Path = self.dataset_path.joinpath(f"{name}.pkl")
        if not file_path.exists():
            logger.warning(f"Dataset file {name} does not exist")
            return None

        with open(file_path, mode="rb") as f:
            dataset: AlphaDataset = pickle.load(f)
            return dataset

    def remove_dataset(self, name: str) -> bool:
        """Remove dataset"""
        file_path: Path = self.dataset_path.joinpath(f"{name}.pkl")
        if not file_path.exists():
            logger.error(f"Dataset file {name} does not exist")
            return False

        file_path.unlink()
        return True

    def list_all_datasets(self) -> list[str]:
        """List all datasets from MongoDB and files"""
        datasets = set()
        
        # 从MongoDB获取
        if self.mongodb_storage:
            try:
                mongodb_datasets = self.mongodb_storage.list_datasets()
                datasets.update(mongodb_datasets)
            except Exception as e:
                logger.warning(f"从MongoDB列出数据集失败: {e}")
        
        # 从文件获取
        file_datasets = [file.stem for file in self.dataset_path.glob("*.pkl")]
        datasets.update(file_datasets)
        
        return sorted(list(datasets))

    def save_model(self, name: str, model: AlphaModel) -> None:
        """Save model to MongoDB or file"""
        # 优先使用MongoDB
        if self.mongodb_storage:
            try:
                if self.mongodb_storage.save_model(name, model):
                    logger.debug(f"模型已保存到MongoDB: {name}")
                    return
            except Exception as e:
                logger.warning(f"MongoDB保存模型失败，回退到文件存储: {e}")
        
        # 回退到文件存储
        file_path: Path = self.model_path.joinpath(f"{name}.pkl")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, mode="wb") as f:
            pickle.dump(model, f)

    def load_model(self, name: str) -> AlphaModel | None:
        """Load model from MongoDB or file"""
        # 优先从MongoDB加载
        if self.mongodb_storage:
            try:
                model = self.mongodb_storage.load_model(name)
                if model:
                    logger.debug(f"从MongoDB加载模型: {name}")
                    return model
            except Exception as e:
                logger.warning(f"从MongoDB加载模型失败，尝试从文件加载: {e}")
        
        # 回退到文件存储
        file_path: Path = self.model_path.joinpath(f"{name}.pkl")
        if not file_path.exists():
            logger.warning(f"Model file {name} does not exist")
            return None

        with open(file_path, mode="rb") as f:
            model: AlphaModel = pickle.load(f)
            return model

    def remove_model(self, name: str) -> bool:
        """Remove model"""
        file_path: Path = self.model_path.joinpath(f"{name}.pkl")
        if not file_path.exists():
            logger.error(f"Model file {name} does not exist")
            return False

        file_path.unlink()
        return True

    def list_all_models(self) -> list[str]:
        """List all models from MongoDB and files"""
        models = set()
        
        # 从MongoDB获取
        if self.mongodb_storage:
            try:
                mongodb_models = self.mongodb_storage.list_models()
                models.update(mongodb_models)
            except Exception as e:
                logger.warning(f"从MongoDB列出模型失败: {e}")
        
        # 从文件获取
        file_models = [file.stem for file in self.model_path.glob("*.pkl")]
        models.update(file_models)
        
        return sorted(list(models))

    def save_signal(self, name: str, signal: pl.DataFrame) -> None:
        """Save signal"""
        file_path: Path = self.signal_path.joinpath(f"{name}.parquet")

        signal.write_parquet(file_path)

    def load_signal(self, name: str) -> pl.DataFrame | None:
        """Load signal"""
        file_path: Path = self.signal_path.joinpath(f"{name}.parquet")
        if not file_path.exists():
            logger.error(f"Signal file {name} does not exist")
            return None

        return pl.read_parquet(file_path)

    def remove_signal(self, name: str) -> bool:
        """Remove signal"""
        file_path: Path = self.signal_path.joinpath(f"{name}.parquet")
        if not file_path.exists():
            logger.error(f"Signal file {name} does not exist")
            return False

        file_path.unlink()
        return True

    def list_all_signals(self) -> list[str]:
        """List all signals"""
        return [file.stem for file in self.model_path.glob("*.parquet")]
