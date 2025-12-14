"""
Database extractors for PostgreSQL, MySQL, and MongoDB
"""
from typing import Dict, Any, Optional, List
import pandas as pd
import pymysql
import pymongo
from sqlalchemy import create_engine
from etl.extractors.base import BaseExtractor
from etl.logger import get_logger

logger = get_logger(__name__)


class PostgreSQLExtractor(BaseExtractor):
    """Extract data from PostgreSQL databases"""
    
    def __init__(self, connection_string: str, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("postgresql", config_dict)
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
    
    def extract(self, query: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> pd.DataFrame:
        """
        Extract data using SQL query
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            DataFrame with query results
        """
        try:
            df = pd.read_sql(query, self.engine, params=params)
            logger.info("postgresql_extraction_successful", rows=len(df))
            return df
            
        except Exception as e:
            logger.error("postgresql_extraction_failed", error=str(e))
            raise
    
    def extract_table(self, table_name: str, schema: Optional[str] = None, 
                     where_clause: Optional[str] = None) -> pd.DataFrame:
        """Extract entire table or with WHERE clause"""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        query = f"SELECT * FROM {full_table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        return self.extract(query)


class MySQLExtractor(BaseExtractor):
    """Extract data from MySQL databases"""
    
    def __init__(self, host: str, database: str, user: str, password: str, 
                 port: int = 3306, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("mysql", config_dict)
        self.connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.connection_string)
    
    def extract(self, query: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> pd.DataFrame:
        """
        Extract data using SQL query
        
        Args:
            query: SQL query string
            params: Query parameters
        """
        try:
            df = pd.read_sql(query, self.engine, params=params)
            logger.info("mysql_extraction_successful", rows=len(df))
            return df
            
        except Exception as e:
            logger.error("mysql_extraction_failed", error=str(e))
            raise
    
    def extract_table(self, table_name: str, where_clause: Optional[str] = None) -> pd.DataFrame:
        """Extract entire table or with WHERE clause"""
        query = f"SELECT * FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        return self.extract(query)


class MongoDBExtractor(BaseExtractor):
    """Extract data from MongoDB collections"""
    
    def __init__(self, connection_string: str, database: str, 
                 config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("mongodb", config_dict)
        self.client = pymongo.MongoClient(connection_string)
        self.database = self.client[database]
    
    def extract(self, collection_name: str, query: Optional[Dict[str, Any]] = None,
                projection: Optional[Dict[str, int]] = None, 
                limit: Optional[int] = None, **kwargs) -> pd.DataFrame:
        """
        Extract data from MongoDB collection
        
        Args:
            collection_name: Name of the collection
            query: MongoDB query filter
            projection: Field projection
            limit: Maximum number of documents
            
        Returns:
            DataFrame with documents
        """
        try:
            collection = self.database[collection_name]
            
            cursor = collection.find(
                filter=query or {},
                projection=projection,
                limit=limit if limit else 0
            )
            
            data = list(cursor)
            
            # Convert ObjectId to string
            for doc in data:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            df = pd.DataFrame(data)
            logger.info("mongodb_extraction_successful", collection=collection_name, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("mongodb_extraction_failed", collection=collection_name, error=str(e))
            raise
    
    def extract_aggregation(self, collection_name: str, 
                           pipeline: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Extract data using aggregation pipeline
        
        Args:
            collection_name: Name of the collection
            pipeline: MongoDB aggregation pipeline
        """
        try:
            collection = self.database[collection_name]
            data = list(collection.aggregate(pipeline))
            
            # Convert ObjectId to string
            for doc in data:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            df = pd.DataFrame(data)
            logger.info("mongodb_aggregation_successful", collection=collection_name, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("mongodb_aggregation_failed", collection=collection_name, error=str(e))
            raise
    
    def __del__(self):
        """Close MongoDB connection"""
        if hasattr(self, 'client'):
            self.client.close()
