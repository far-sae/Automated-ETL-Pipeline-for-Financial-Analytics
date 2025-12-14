"""
Configuration management for the ETL pipeline
"""
import os
from typing import Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration"""
    host: str = os.getenv('POSTGRES_HOST', 'localhost')
    port: int = int(os.getenv('POSTGRES_PORT', 5432))
    database: str = os.getenv('POSTGRES_DB', 'financial_analytics')
    user: str = os.getenv('POSTGRES_USER', 'etl_user')
    password: str = os.getenv('POSTGRES_PASSWORD', '')
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class RedisConfig:
    """Redis configuration"""
    host: str = os.getenv('REDIS_HOST', 'localhost')
    port: int = int(os.getenv('REDIS_PORT', 6379))
    password: str = os.getenv('REDIS_PASSWORD', '')
    db: int = 0


@dataclass
class S3Config:
    """AWS S3 configuration"""
    access_key_id: str = os.getenv('AWS_ACCESS_KEY_ID', '')
    secret_access_key: str = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    region: str = os.getenv('AWS_REGION', 'us-east-1')
    bucket: str = os.getenv('S3_BUCKET', 'financial-etl-staging')
    prefix: str = os.getenv('S3_PREFIX', 'staging/')


@dataclass
class ETLConfig:
    """ETL pipeline configuration"""
    batch_size: int = int(os.getenv('ETL_BATCH_SIZE', 10000))
    max_workers: int = int(os.getenv('ETL_MAX_WORKERS', 4))
    retry_count: int = int(os.getenv('ETL_RETRY_COUNT', 3))
    retry_delay: int = int(os.getenv('ETL_RETRY_DELAY', 300))
    validation_strict_mode: bool = os.getenv('VALIDATION_STRICT_MODE', 'true').lower() == 'true'
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')


class Config:
    """Main configuration class"""
    
    def __init__(self):
        self.database = DatabaseConfig()
        self.redis = RedisConfig()
        self.s3 = S3Config()
        self.etl = ETLConfig()
        
    def get_api_credentials(self, provider: str) -> Dict[str, Any]:
        """Get API credentials for a specific provider"""
        credentials = {
            'alpha_vantage': {
                'api_key': os.getenv('ALPHA_VANTAGE_API_KEY', '')
            },
            'finnhub': {
                'api_key': os.getenv('FINNHUB_API_KEY', '')
            },
            'yahoo_finance': {
                'api_key': os.getenv('YAHOO_FINANCE_API_KEY', '')
            },
            'iex_cloud': {
                'api_key': os.getenv('IEX_CLOUD_API_KEY', '')
            }
        }
        return credentials.get(provider, {})


# Global config instance
config = Config()
