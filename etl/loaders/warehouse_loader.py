"""
PostgreSQL data warehouse loader with bulk operations
"""
from typing import Dict, Any, Optional, List
import pandas as pd
from etl.loaders.base import BaseLoader
from etl.utils import DatabaseConnection, DistributedLock
from etl.config import config
from etl.logger import get_logger

logger = get_logger(__name__)


class WarehouseLoader(BaseLoader):
    """Load data to PostgreSQL data warehouse using bulk operations"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("warehouse_loader", config_dict)
        self.db = DatabaseConnection()
    
    def load(self, df: pd.DataFrame, **kwargs) -> int:
        """
        Load DataFrame to warehouse table
        
        Kwargs:
            table_name: Target table name (with schema)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            use_lock: Whether to use distributed locking
            lock_name: Name for distributed lock
        """
        table_name = kwargs.get('table_name')
        if not table_name:
            raise ValueError("table_name is required")
        
        if_exists = kwargs.get('if_exists', 'append')
        use_lock = kwargs.get('use_lock', True)
        lock_name = kwargs.get('lock_name', f'load_{table_name}')
        
        if use_lock:
            with DistributedLock(lock_name).context():
                return self._perform_load(df, table_name, if_exists)
        else:
            return self._perform_load(df, table_name, if_exists)
    
    def _perform_load(self, df: pd.DataFrame, table_name: str, if_exists: str) -> int:
        """Perform the actual load operation"""
        try:
            loaded_count = self.db.load_dataframe(df, table_name, if_exists=if_exists)
            logger.info("warehouse_load_successful", table=table_name, rows=loaded_count)
            return loaded_count
            
        except Exception as e:
            logger.error("warehouse_load_failed", table=table_name, error=str(e))
            raise
    
    def bulk_insert(self, data: List[Dict[str, Any]], table_name: str, 
                   batch_size: Optional[int] = None) -> int:
        """
        Bulk insert using execute_batch for optimal performance
        
        Args:
            data: List of dictionaries representing rows
            table_name: Target table name
            batch_size: Number of rows per batch
        """
        if not batch_size:
            batch_size = config.etl.batch_size
        
        try:
            inserted = self.db.bulk_insert(table_name, data, batch_size=batch_size)
            logger.info("bulk_insert_successful", table=table_name, rows=inserted)
            return inserted
            
        except Exception as e:
            logger.error("bulk_insert_failed", table=table_name, error=str(e))
            raise
    
    def upsert(self, df: pd.DataFrame, table_name: str, 
              conflict_columns: List[str], update_columns: Optional[List[str]] = None) -> int:
        """
        Upsert data (INSERT ... ON CONFLICT DO UPDATE)
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            conflict_columns: Columns to check for conflicts
            update_columns: Columns to update on conflict (defaults to all except conflict columns)
        """
        if update_columns is None:
            update_columns = [col for col in df.columns if col not in conflict_columns]
        
        # Convert DataFrame to list of dicts
        data = df.to_dict('records')
        
        # Build upsert query
        columns = list(df.columns)
        placeholders = ','.join(['%s'] * len(columns))
        column_names = ','.join(columns)
        
        conflict_clause = ','.join(conflict_columns)
        update_clause = ','.join([f"{col}=EXCLUDED.{col}" for col in update_columns])
        
        query = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_clause})
            DO UPDATE SET {update_clause}
        """
        
        values = [[row.get(col) for col in columns] for row in data]
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    from psycopg2.extras import execute_batch
                    execute_batch(cursor, query, values, page_size=config.etl.batch_size)
                    upserted = cursor.rowcount
            
            logger.info("upsert_successful", table=table_name, rows=upserted)
            return upserted
            
        except Exception as e:
            logger.error("upsert_failed", table=table_name, error=str(e))
            raise
    
    def truncate_and_load(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Truncate table and load fresh data
        
        Args:
            df: DataFrame to load
            table_name: Target table name
        """
        try:
            # Truncate table
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {table_name}")
            
            logger.info("table_truncated", table=table_name)
            
            # Load data
            loaded = self.db.load_dataframe(df, table_name, if_exists='append')
            logger.info("truncate_and_load_successful", table=table_name, rows=loaded)
            
            return loaded
            
        except Exception as e:
            logger.error("truncate_and_load_failed", table=table_name, error=str(e))
            raise
