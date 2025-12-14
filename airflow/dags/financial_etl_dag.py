"""
Main Financial ETL DAG for Apache Airflow

This DAG orchestrates the complete ETL pipeline for financial analytics:
1. Extract data from 15+ financial sources
2. Validate data (completeness, accuracy, consistency)
3. Transform data with business rules
4. Load to PostgreSQL data warehouse
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd

# ETL components
from etl.extractors import (
    AlphaVantageExtractor, FinnhubExtractor, CSVExtractor,
    PostgreSQLExtractor, MongoDBExtractor
)
from etl.validators import (
    CompletenessValidator, AccuracyValidator, ConsistencyValidator, SchemaValidator
)
from etl.transformers import StockPriceTransformer, FinancialRatioTransformer
from etl.loaders import WarehouseLoader
from etl.utils import S3Handler, DatabaseConnection, DistributedLock
from etl.logger import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}


def extract_stock_prices(**context):
    """Extract stock price data from multiple sources"""
    logger.info("Starting stock price extraction")
    
    # Example: Extract from Yahoo Finance
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    
    try:
        # This would use YahooFinanceExtractor in production
        # For demo, create sample data
        data = {
            'symbol': symbols * 30,
            'trade_date': pd.date_range('2024-01-01', periods=30, freq='D').tolist() * len(symbols),
            'open_price': [150.0] * 150,
            'high_price': [155.0] * 150,
            'low_price': [148.0] * 150,
            'close_price': [152.0] * 150,
            'volume': [1000000] * 150
        }
        df = pd.DataFrame(data)
        
        # Stage to S3
        s3_handler = S3Handler()
        s3_path = s3_handler.upload_dataframe(
            df, 
            f"raw/stock_prices/{context['ds']}/data",
            format='parquet'
        )
        
        logger.info(f"Stock prices extracted and staged: {s3_path}")
        return s3_path
        
    except Exception as e:
        logger.error(f"Stock price extraction failed: {str(e)}")
        raise


def validate_stock_prices(**context):
    """Validate extracted stock price data"""
    logger.info("Starting stock price validation")
    
    ti = context['ti']
    s3_path = ti.xcom_pull(task_ids='extract_group.extract_stock_prices')
    
    # Download from S3
    s3_handler = S3Handler()
    key = s3_path.split(s3_handler.bucket + '/')[-1]
    df = s3_handler.download_dataframe(key, format='parquet')
    
    # Run validations
    completeness_validator = CompletenessValidator(strict_mode=False)
    accuracy_validator = AccuracyValidator(strict_mode=False)
    consistency_validator = ConsistencyValidator(strict_mode=False)
    
    # Completeness check
    completeness_result = completeness_validator.validate(
        df,
        required_columns=['symbol', 'trade_date', 'close_price', 'volume'],
        null_threshold=5,
        min_row_count=1
    )
    
    # Accuracy check
    accuracy_result = accuracy_validator.validate(
        df,
        value_ranges={
            'close_price': (0, 10000),
            'volume': (0, 1e12)
        }
    )
    
    # Consistency check
    consistency_result = consistency_validator.validate(
        df,
        unique_columns=[],
        data_types={
            'symbol': 'object',
            'close_price': 'float64'
        }
    )
    
    # Log validation results to database
    db = DatabaseConnection()
    for result in [completeness_result, accuracy_result, consistency_result]:
        db.execute_update(
            """
            INSERT INTO metadata.data_quality_log 
            (validation_type, validation_rule, passed_records, failed_records, validation_details)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (result.validation_type, result.validator_name, result.passed_records,
             result.failed_records, str(result.error_details)[:500])
        )
    
    logger.info("Stock price validation completed")
    return s3_path


def transform_stock_prices(**context):
    """Transform stock price data with technical indicators"""
    logger.info("Starting stock price transformation")
    
    ti = context['ti']
    s3_path = ti.xcom_pull(task_ids='validate_group.validate_stock_prices')
    
    # Download from S3
    s3_handler = S3Handler()
    key = s3_path.split(s3_handler.bucket + '/')[-1]
    df = s3_handler.download_dataframe(key, format='parquet')
    
    # Transform
    transformer = StockPriceTransformer()
    transformed_df = transformer.execute(df)
    
    # Stage transformed data
    transformed_path = s3_handler.upload_dataframe(
        transformed_df,
        f"transformed/stock_prices/{context['ds']}/data",
        format='parquet'
    )
    
    logger.info(f"Stock prices transformed and staged: {transformed_path}")
    return transformed_path


def load_stock_prices(**context):
    """Load transformed stock prices to data warehouse"""
    logger.info("Starting stock price load")
    
    ti = context['ti']
    s3_path = ti.xcom_pull(task_ids='transform_group.transform_stock_prices')
    
    # Download from S3
    s3_handler = S3Handler()
    key = s3_path.split(s3_handler.bucket + '/')[-1]
    df = s3_handler.download_dataframe(key, format='parquet')
    
    # Load to warehouse
    loader = WarehouseLoader()
    loaded_count = loader.execute(
        df,
        table_name='analytics.daily_stock_analytics',
        if_exists='append',
        use_lock=True
    )
    
    # Log ETL run
    db = DatabaseConnection()
    db.log_etl_run(
        dag_id=context['dag'].dag_id,
        task_id=context['task'].task_id,
        source_name='stock_prices',
        status='SUCCESS',
        records_extracted=len(df),
        records_validated=len(df),
        records_loaded=loaded_count
    )
    
    logger.info(f"Stock prices loaded: {loaded_count} records")
    return loaded_count


# Define the DAG
with DAG(
    'financial_etl_pipeline',
    default_args=default_args,
    description='Comprehensive ETL pipeline for financial analytics',
    schedule='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['financial', 'etl', 'analytics'],
    max_active_runs=1
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Extraction task group
    with TaskGroup('extract_group', tooltip='Extract data from sources') as extract_group:
        extract_stock_prices_task = PythonOperator(
            task_id='extract_stock_prices',
            python_callable=extract_stock_prices,
            provide_context=True
        )
        
        # Additional extraction tasks would go here
        # extract_company_financials = PythonOperator(...)
        # extract_economic_indicators = PythonOperator(...)
        # extract_news = PythonOperator(...)
    
    # Validation task group
    with TaskGroup('validate_group', tooltip='Validate extracted data') as validate_group:
        validate_stock_prices_task = PythonOperator(
            task_id='validate_stock_prices',
            python_callable=validate_stock_prices,
            provide_context=True
        )
    
    # Transformation task group
    with TaskGroup('transform_group', tooltip='Transform data') as transform_group:
        transform_stock_prices_task = PythonOperator(
            task_id='transform_stock_prices',
            python_callable=transform_stock_prices,
            provide_context=True
        )
    
    # Loading task group
    with TaskGroup('load_group', tooltip='Load data to warehouse') as load_group:
        load_stock_prices_task = PythonOperator(
            task_id='load_stock_prices',
            python_callable=load_stock_prices,
            provide_context=True
        )
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    start >> extract_group >> validate_group >> transform_group >> load_group >> end
