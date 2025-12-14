# Quick Start Guide - Financial ETL Pipeline

## Prerequisites

- Docker Desktop installed and running
- Docker Compose v1.29 or higher
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space

## Installation Steps

### 1. Clone and Setup

```bash
# Navigate to project directory
cd "Automated ETL Pipeline for Financial Analytics"

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env  # or use your preferred editor
```

### 2. Configure Environment Variables

Edit `.env` and set at minimum:

```bash
# Required
POSTGRES_PASSWORD=your_secure_password
REDIS_PASSWORD=your_redis_password

# For S3 (if using)
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET=your-bucket-name

# For API sources (optional)
ALPHA_VANTAGE_API_KEY=your_api_key
FINNHUB_API_KEY=your_api_key
```

### 3. Deploy the Pipeline

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Deploy entire stack
./scripts/deploy.sh
```

This will:
- Build Docker images
- Start all services (PostgreSQL, Redis, Airflow, ETL worker)
- Initialize database schema
- Create default Airflow user
- Verify service health

### 4. Access the System

**Airflow Web UI:**
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

**PostgreSQL:**
- Host: `localhost`
- Port: `5432`
- Database: `financial_analytics`
- User: `etl_user`

**Redis:**
- Host: `localhost`
- Port: `6379`

## Running Your First ETL Job

### Option 1: Via Airflow UI

1. Open http://localhost:8080
2. Login with admin/admin
3. Find DAG: `financial_etl_pipeline`
4. Toggle the DAG to "On"
5. Click "Trigger DAG" (play button)
6. Monitor execution in Graph or Grid view

### Option 2: Via Command Line

```bash
# Trigger the DAG
docker-compose exec airflow-scheduler \
  airflow dags trigger financial_etl_pipeline

# Check DAG status
docker-compose exec airflow-scheduler \
  airflow dags list-runs -d financial_etl_pipeline

# View task logs
docker-compose exec airflow-scheduler \
  airflow tasks logs financial_etl_pipeline extract_stock_prices 2024-01-15
```

### Option 3: Python Script

```python
from etl.extractors import CSVExtractor
from etl.validators import CompletenessValidator, AccuracyValidator
from etl.transformers import StockPriceTransformer
from etl.loaders import WarehouseLoader

# Extract
extractor = CSVExtractor()
df = extractor.extract('/path/to/stock_data.csv')

# Validate
validator = CompletenessValidator()
result = validator.validate(
    df,
    required_columns=['symbol', 'close_price', 'volume'],
    null_threshold=5
)
print(f"Validation passed: {result.passed}")

# Transform
transformer = StockPriceTransformer()
transformed_df = transformer.execute(df)

# Load
loader = WarehouseLoader()
loaded = loader.execute(
    transformed_df,
    table_name='analytics.daily_stock_analytics'
)
print(f"Loaded {loaded} records")
```

## Verify Installation

### Check Services

```bash
# Check all services
docker-compose ps

# Expected output: All services "Up" and "healthy"
```

### Query Database

```bash
# Connect to database
docker-compose exec postgres psql -U etl_user -d financial_analytics

# Check schemas
\dn

# Check tables
\dt raw.*
\dt analytics.*
\dt metadata.*

# Sample query
SELECT * FROM metadata.etl_run_log ORDER BY run_start_time DESC LIMIT 5;
```

### Test ETL Components

```bash
# Enter ETL worker container
docker-compose exec etl-worker python

# Test imports
>>> from etl.extractors import CSVExtractor
>>> from etl.validators import CompletenessValidator
>>> from etl.transformers import StockPriceTransformer
>>> from etl.loaders import WarehouseLoader
>>> print("All components loaded successfully!")
```

## Common Operations

### Start/Stop Services

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart specific service
docker-compose restart airflow-scheduler

# View logs
docker-compose logs -f airflow-scheduler
```

### Manage Data

```bash
# Backup database
docker-compose exec postgres pg_dump -U etl_user financial_analytics > backup.sql

# Restore database
docker-compose exec -T postgres psql -U etl_user financial_analytics < backup.sql

# Clear staging data
docker-compose exec postgres psql -U etl_user -d financial_analytics \
  -c "TRUNCATE TABLE staging.temp_data;"
```

### Monitor Pipeline

```bash
# View Airflow logs
docker-compose logs -f airflow-scheduler

# View ETL worker logs
docker-compose logs -f etl-worker

# Check Redis connections
docker-compose exec redis redis-cli INFO clients

# Check PostgreSQL connections
docker-compose exec postgres psql -U etl_user -d financial_analytics \
  -c "SELECT count(*) FROM pg_stat_activity;"
```

## Example Workflows

### Workflow 1: Load CSV Data

```python
# create_load_csv_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def load_csv_data(**context):
    from etl.extractors import CSVExtractor
    from etl.loaders import WarehouseLoader
    
    # Extract
    extractor = CSVExtractor()
    df = extractor.extract('/data/market_data.csv')
    
    # Load
    loader = WarehouseLoader()
    loaded = loader.execute(df, table_name='raw.market_data')
    print(f"Loaded {loaded} records")

with DAG(
    'load_csv_workflow',
    default_args={'retries': 3},
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    load_task = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv_data
    )
```

### Workflow 2: API to Analytics

```python
def api_to_analytics(**context):
    from etl.extractors import AlphaVantageExtractor
    from etl.validators import CompletenessValidator
    from etl.transformers import StockPriceTransformer
    from etl.loaders import WarehouseLoader
    
    # Extract from API
    extractor = AlphaVantageExtractor()
    df = extractor.extract(symbols=['AAPL', 'GOOGL', 'MSFT'])
    
    # Validate
    validator = CompletenessValidator()
    validator.validate(df, required_columns=['symbol', 'close_price'])
    
    # Transform
    transformer = StockPriceTransformer()
    analytics_df = transformer.execute(df)
    
    # Load
    loader = WarehouseLoader()
    loader.execute(analytics_df, table_name='analytics.daily_stock_analytics')
```

## Troubleshooting

### Issue: Airflow UI not accessible

```bash
# Check Airflow webserver logs
docker-compose logs airflow-webserver

# Restart webserver
docker-compose restart airflow-webserver

# Wait 30 seconds and try again
```

### Issue: Database connection failed

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Check logs
docker-compose logs postgres

# Test connection
docker-compose exec postgres pg_isready -U etl_user
```

### Issue: Out of memory

```bash
# Check Docker resources
docker stats

# Increase Docker Desktop memory allocation
# Docker Desktop → Settings → Resources → Memory → 8GB+
```

### Issue: DAG not appearing in Airflow

```bash
# Check DAG file syntax
docker-compose exec airflow-scheduler python /opt/airflow/dags/your_dag.py

# Refresh DAGs
docker-compose exec airflow-scheduler airflow dags list-import-errors

# Restart scheduler
docker-compose restart airflow-scheduler
```

## Next Steps

1. **Customize Data Sources**
   - Edit `config/data_sources.py`
   - Add your API credentials
   - Configure database connections

2. **Create Custom DAGs**
   - Copy example DAG: `airflow/dags/financial_etl_dag.py`
   - Modify for your use case
   - Place in `airflow/dags/` directory

3. **Add Validation Rules**
   - Define in `config/data_sources.py`
   - Customize thresholds and rules
   - Test with sample data

4. **Extend Transformations**
   - Create custom transformer in `etl/transformers/`
   - Implement business logic
   - Add to DAG pipeline

5. **Monitor & Optimize**
   - Review `metadata.etl_run_log` for performance
   - Check `metadata.data_quality_log` for issues
   - Adjust batch sizes and workers

## Additional Resources

- **Architecture**: See `ARCHITECTURE.md` for technical details
- **README**: See `README.md` for complete documentation
- **API Reference**: Check docstrings in Python modules
- **Airflow Docs**: https://airflow.apache.org/docs/
- **PostgreSQL Docs**: https://www.postgresql.org/docs/

## Getting Help

1. Check logs: `docker-compose logs [service_name]`
2. Review Airflow UI task logs
3. Query metadata tables for ETL run history
4. Check individual component tests in `tests/`

## Success Criteria

Your installation is successful when:
- ✅ All services show "Up (healthy)" in `docker-compose ps`
- ✅ Airflow UI accessible at http://localhost:8080
- ✅ DAG `financial_etl_pipeline` visible in Airflow
- ✅ Database schemas created (raw, analytics, metadata)
- ✅ Sample DAG run completes successfully

Welcome to your Financial ETL Pipeline! 🚀
