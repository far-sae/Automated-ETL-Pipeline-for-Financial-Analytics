"""
Base transformer class
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd
from etl.logger import get_logger

logger = get_logger(__name__)


class BaseTransformer(ABC):
    """Abstract base class for data transformers"""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize transformer
        
        Args:
            name: Name of the transformer
            config: Configuration dictionary
        """
        self.name = name
        self.config = config or {}
        self.transformed_count = 0
        self.start_time = None
        self.end_time = None
    
    @abstractmethod
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform DataFrame
        
        Args:
            df: Input DataFrame
            **kwargs: Additional transformation parameters
            
        Returns:
            Transformed DataFrame
        """
        pass
    
    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Hook called before transformation"""
        self.start_time = datetime.now()
        logger.info(
            "transformation_started",
            transformer=self.name,
            input_records=len(df),
            start_time=self.start_time.isoformat()
        )
        return df
    
    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Hook called after transformation"""
        self.end_time = datetime.now()
        self.transformed_count = len(df)
        
        duration = (self.end_time - self.start_time).total_seconds()
        
        logger.info(
            "transformation_completed",
            transformer=self.name,
            output_records=self.transformed_count,
            duration_seconds=duration
        )
        
        return df
    
    def execute(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Execute complete transformation pipeline
        
        Args:
            df: Input DataFrame
            **kwargs: Transformation parameters
            
        Returns:
            Transformed DataFrame
        """
        df = self.pre_transform(df)
        df = self.transform(df, **kwargs)
        df = self.post_transform(df)
        return df
