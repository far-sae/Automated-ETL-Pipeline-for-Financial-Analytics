"""
S3 handler for staging data operations
"""
import io
import json
from typing import Any, Optional, List
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from etl.config import config
from etl.logger import get_logger

logger = get_logger(__name__)


class S3Handler:
    """Handle S3 operations for data staging"""
    
    def __init__(self):
        """Initialize S3 client"""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.s3.access_key_id,
            aws_secret_access_key=config.s3.secret_access_key,
            region_name=config.s3.region
        )
        self.bucket = config.s3.bucket
        self.prefix = config.s3.prefix
    
    def upload_dataframe(self, df: pd.DataFrame, key: str, format: str = 'parquet') -> str:
        """
        Upload pandas DataFrame to S3
        
        Args:
            df: DataFrame to upload
            key: S3 key (without prefix)
            format: File format ('parquet', 'csv', 'json')
            
        Returns:
            Full S3 path
        """
        full_key = f"{self.prefix}{key}"
        
        try:
            buffer = io.BytesIO()
            
            if format == 'parquet':
                df.to_parquet(buffer, index=False, engine='pyarrow')
                full_key = f"{full_key}.parquet"
            elif format == 'csv':
                df.to_csv(buffer, index=False)
                full_key = f"{full_key}.csv"
            elif format == 'json':
                df.to_json(buffer, orient='records', lines=True)
                full_key = f"{full_key}.jsonl"
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            buffer.seek(0)
            self.s3_client.upload_fileobj(buffer, self.bucket, full_key)
            
            s3_path = f"s3://{self.bucket}/{full_key}"
            logger.info("dataframe_uploaded", s3_path=s3_path, rows=len(df), format=format)
            
            return s3_path
            
        except Exception as e:
            logger.error("dataframe_upload_failed", key=key, error=str(e))
            raise
    
    def download_dataframe(self, key: str, format: str = 'parquet') -> pd.DataFrame:
        """
        Download DataFrame from S3
        
        Args:
            key: S3 key (with or without prefix)
            format: File format ('parquet', 'csv', 'json')
            
        Returns:
            pandas DataFrame
        """
        if not key.startswith(self.prefix):
            key = f"{self.prefix}{key}"
        
        try:
            buffer = io.BytesIO()
            self.s3_client.download_fileobj(self.bucket, key, buffer)
            buffer.seek(0)
            
            if format == 'parquet':
                df = pd.read_parquet(buffer, engine='pyarrow')
            elif format == 'csv':
                df = pd.read_csv(buffer)
            elif format == 'json':
                df = pd.read_json(buffer, lines=True)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info("dataframe_downloaded", key=key, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("dataframe_download_failed", key=key, error=str(e))
            raise
    
    def upload_json(self, data: Any, key: str) -> str:
        """Upload JSON data to S3"""
        full_key = f"{self.prefix}{key}.json"
        
        try:
            json_data = json.dumps(data, default=str)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=full_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            s3_path = f"s3://{self.bucket}/{full_key}"
            logger.info("json_uploaded", s3_path=s3_path)
            return s3_path
            
        except Exception as e:
            logger.error("json_upload_failed", key=key, error=str(e))
            raise
    
    def download_json(self, key: str) -> Any:
        """Download JSON data from S3"""
        if not key.startswith(self.prefix):
            key = f"{self.prefix}{key}"
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response['Body'].read())
            logger.info("json_downloaded", key=key)
            return data
            
        except Exception as e:
            logger.error("json_download_failed", key=key, error=str(e))
            raise
    
    def list_objects(self, prefix: Optional[str] = None) -> List[str]:
        """List objects in S3 bucket"""
        list_prefix = f"{self.prefix}{prefix}" if prefix else self.prefix
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=list_prefix
            )
            
            keys = [obj['Key'] for obj in response.get('Contents', [])]
            logger.info("objects_listed", prefix=list_prefix, count=len(keys))
            return keys
            
        except Exception as e:
            logger.error("list_objects_failed", prefix=list_prefix, error=str(e))
            raise
    
    def delete_object(self, key: str) -> bool:
        """Delete object from S3"""
        if not key.startswith(self.prefix):
            key = f"{self.prefix}{key}"
        
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)
            logger.info("object_deleted", key=key)
            return True
            
        except Exception as e:
            logger.error("delete_object_failed", key=key, error=str(e))
            return False
