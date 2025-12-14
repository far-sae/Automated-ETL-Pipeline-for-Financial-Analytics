"""
Base extractor class for all data sources
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd
from etl.logger import get_logger
from etl.utils import S3Handler

logger = get_logger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for all data extractors"""
    
    def __init__(self, source_name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize base extractor
        
        Args:
            source_name: Name of the data source
            config: Configuration dictionary for the extractor
        """
        self.source_name = source_name
        self.config = config or {}
        self.s3_handler = S3Handler()
        self.extracted_count = 0
        self.start_time = None
        self.end_time = None
    
    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data from the source
        
        Returns:
            DataFrame containing extracted data
        """
        pass
    
    def validate_config(self) -> bool:
        """Validate extractor configuration"""
        return True
    
    def pre_extract(self) -> None:
        """Hook called before extraction"""
        self.start_time = datetime.now()
        logger.info(
            "extraction_started",
            source=self.source_name,
            start_time=self.start_time.isoformat()
        )
    
    def post_extract(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Hook called after extraction
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Processed DataFrame
        """
        self.end_time = datetime.now()
        self.extracted_count = len(df)
        
        duration = (self.end_time - self.start_time).total_seconds()
        
        logger.info(
            "extraction_completed",
            source=self.source_name,
            records=self.extracted_count,
            duration_seconds=duration
        )
        
        # Add metadata columns
        df['source_system'] = self.source_name
        df['extracted_at'] = self.end_time
        
        return df
    
    def extract_and_stage(self, s3_key: str, **kwargs) -> str:
        """
        Extract data and upload to S3 staging area
        
        Args:
            s3_key: S3 key for staging file
            **kwargs: Arguments passed to extract method
            
        Returns:
            S3 path of staged data
        """
        self.pre_extract()
        df = self.extract(**kwargs)
        df = self.post_extract(df)
        
        # Upload to S3
        s3_path = self.s3_handler.upload_dataframe(df, s3_key, format='parquet')
        
        logger.info(
            "data_staged",
            source=self.source_name,
            s3_path=s3_path,
            records=len(df)
        )
        
        return s3_path
    
    def get_extraction_metadata(self) -> Dict[str, Any]:
        """Get metadata about the extraction"""
        return {
            'source_name': self.source_name,
            'extracted_count': self.extracted_count,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': (
                (self.end_time - self.start_time).total_seconds()
                if self.start_time and self.end_time else None
            )
        }
