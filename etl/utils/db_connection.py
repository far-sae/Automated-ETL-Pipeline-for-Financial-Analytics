"""
Database connection management
"""
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import pandas as pd
from etl.config import config
from etl.logger import get_logger

logger = get_logger(__name__)


class DatabaseConnection:
    """Manage PostgreSQL database connections"""
    
    def __init__(self):
        """Initialize database connection pool"""
        self.engine = create_engine(
            config.database.connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
    
    @contextmanager
    def get_connection(self):
        """Get a database connection from pool"""
        conn = self.engine.raw_connection()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("database_error", error=str(e))
            raise
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute INSERT/UPDATE/DELETE query"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.rowcount
    
    def bulk_insert(self, table: str, data: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """
        Bulk insert data using execute_batch
        
        Args:
            table: Table name (with schema)
            data: List of dictionaries representing rows
            batch_size: Number of rows per batch
            
        Returns:
            Number of rows inserted
        """
        if not data:
            return 0
        
        columns = list(data[0].keys())
        placeholders = ','.join(['%s'] * len(columns))
        column_names = ','.join(columns)
        
        query = f"""
            INSERT INTO {table} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """
        
        values = [[row.get(col) for col in columns] for row in data]
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor, query, values, page_size=batch_size)
                inserted = cursor.rowcount
                
        logger.info("bulk_insert_complete", table=table, rows=inserted)
        return inserted
    
    def load_dataframe(self, df: pd.DataFrame, table: str, if_exists: str = 'append') -> int:
        """
        Load pandas DataFrame to database table
        
        Args:
            df: DataFrame to load
            table: Table name (with schema)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            
        Returns:
            Number of rows loaded
        """
        try:
            rows = df.to_sql(
                name=table.split('.')[-1],
                schema=table.split('.')[0] if '.' in table else None,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=config.etl.batch_size
            )
            
            logger.info("dataframe_loaded", table=table, rows=len(df))
            return len(df)
            
        except Exception as e:
            logger.error("dataframe_load_failed", table=table, error=str(e))
            raise
    
    def read_to_dataframe(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        try:
            df = pd.read_sql(query, self.engine, params=params)
            logger.info("query_executed", rows=len(df))
            return df
            
        except Exception as e:
            logger.error("query_execution_failed", error=str(e))
            raise
    
    def log_etl_run(self, dag_id: str, task_id: str, source_name: str, 
                    status: str, records_extracted: Optional[int] = None,
                    records_validated: Optional[int] = None,
                    records_loaded: Optional[int] = None,
                    error_message: Optional[str] = None) -> int:
        """Log ETL run metadata"""
        query = """
            INSERT INTO metadata.etl_run_log 
            (dag_id, task_id, source_name, status, records_extracted, 
             records_validated, records_loaded, error_message, run_end_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING run_id
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    dag_id, task_id, source_name, status,
                    records_extracted, records_validated, records_loaded, error_message
                ))
                run_id = cursor.fetchone()[0]
                
        return run_id
