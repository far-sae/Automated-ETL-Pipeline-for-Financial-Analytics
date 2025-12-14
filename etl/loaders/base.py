"""
Base loader class
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd
from etl.logger import get_logger

logger = get_logger(__name__)


class BaseLoader(ABC):
    """Abstract base class for data loaders"""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize loader
        
        Args:
            name: Name of the loader
            config: Configuration dictionary
        """
        self.name = name
        self.config = config or {}
        self.loaded_count = 0
        self.start_time = None
        self.end_time = None
    
    @abstractmethod
    def load(self, df: pd.DataFrame, **kwargs) -> int:
        """
        Load DataFrame to destination
        
        Args:
            df: DataFrame to load
            **kwargs: Additional load parameters
            
        Returns:
            Number of records loaded
        """
        pass
    
    def pre_load(self, df: pd.DataFrame) -> pd.DataFrame:
        """Hook called before loading"""
        self.start_time = datetime.now()
        logger.info(
            "load_started",
            loader=self.name,
            records=len(df),
            start_time=self.start_time.isoformat()
        )
        return df
    
    def post_load(self, loaded_count: int) -> None:
        """Hook called after loading"""
        self.end_time = datetime.now()
        self.loaded_count = loaded_count
        
        duration = (self.end_time - self.start_time).total_seconds()
        
        logger.info(
            "load_completed",
            loader=self.name,
            records_loaded=self.loaded_count,
            duration_seconds=duration,
            records_per_second=round(self.loaded_count / duration, 2) if duration > 0 else 0
        )
    
    def execute(self, df: pd.DataFrame, **kwargs) -> int:
        """
        Execute complete load pipeline
        
        Args:
            df: DataFrame to load
            **kwargs: Load parameters
            
        Returns:
            Number of records loaded
        """
        df = self.pre_load(df)
        loaded_count = self.load(df, **kwargs)
        self.post_load(loaded_count)
        return loaded_count
