"""
Data enrichment transformers
"""
from typing import Dict, Any, Optional, List
import pandas as pd
from etl.transformers.base import BaseTransformer
from etl.logger import get_logger

logger = get_logger(__name__)


class DataEnricher(BaseTransformer):
    """Enrich data with additional information"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("data_enricher", config)
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Enrich data with lookups and derived fields
        
        Kwargs:
            lookup_data: Dict of lookup DataFrames for joining
            derived_columns: Dict of column_name: calculation_function
        """
        df = df.copy()
        
        lookup_data = kwargs.get('lookup_data', {})
        derived_columns = kwargs.get('derived_columns', {})
        
        # Perform lookups/joins
        for lookup_name, lookup_df in lookup_data.items():
            join_key = kwargs.get(f'{lookup_name}_join_key', 'id')
            if join_key in df.columns:
                df = df.merge(lookup_df, on=join_key, how='left', suffixes=('', f'_{lookup_name}'))
                logger.info("enrichment_lookup_applied", lookup=lookup_name, rows=len(df))
        
        # Calculate derived columns
        for col_name, calc_func in derived_columns.items():
            try:
                df[col_name] = calc_func(df)
                logger.info("derived_column_added", column=col_name)
            except Exception as e:
                logger.error("derived_column_failed", column=col_name, error=str(e))
        
        return df
    
    def enrich_with_metadata(self, df: pd.DataFrame, 
                            source_system: str,
                            load_timestamp: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """Add metadata columns"""
        df = df.copy()
        
        df['source_system'] = source_system
        df['load_timestamp'] = load_timestamp or pd.Timestamp.now()
        df['record_hash'] = pd.util.hash_pandas_object(df, index=False)
        
        return df
