"""
Data aggregation transformers
"""
from typing import Dict, Any, Optional, List
import pandas as pd
from etl.transformers.base import BaseTransformer
from etl.logger import get_logger

logger = get_logger(__name__)


class DataAggregator(BaseTransformer):
    """Aggregate data for analysis"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("data_aggregator", config)
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Aggregate data
        
        Kwargs:
            group_by: List of columns to group by
            aggregations: Dict of column: aggregation_function(s)
        """
        group_by = kwargs.get('group_by', [])
        aggregations = kwargs.get('aggregations', {})
        
        if not group_by or not aggregations:
            logger.warning("aggregation_skipped", reason="missing_parameters")
            return df
        
        try:
            agg_df = df.groupby(group_by).agg(aggregations).reset_index()
            
            # Flatten multi-level columns if present
            if isinstance(agg_df.columns, pd.MultiIndex):
                agg_df.columns = ['_'.join(col).strip('_') for col in agg_df.columns.values]
            
            logger.info("aggregation_completed", 
                       input_rows=len(df),
                       output_rows=len(agg_df),
                       group_by=group_by)
            
            return agg_df
            
        except Exception as e:
            logger.error("aggregation_failed", error=str(e))
            raise
    
    def time_series_aggregation(self, df: pd.DataFrame,
                                date_column: str,
                                frequency: str = 'D',
                                value_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Aggregate time series data
        
        Args:
            df: Input DataFrame
            date_column: Column containing dates
            frequency: Aggregation frequency ('D', 'W', 'M', 'Q', 'Y')
            value_columns: Columns to aggregate
        """
        df = df.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        df.set_index(date_column, inplace=True)
        
        if value_columns is None:
            value_columns = df.select_dtypes(include=['number']).columns.tolist()
        
        agg_df = df[value_columns].resample(frequency).agg({
            col: ['mean', 'sum', 'min', 'max', 'count'] for col in value_columns
        })
        
        agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]
        agg_df.reset_index(inplace=True)
        
        return agg_df
