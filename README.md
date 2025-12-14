# Automated ETL Pipeline for Financial Analytics

A comprehensive, production-ready ETL pipeline designed for financial analytics with support for 15+ data sources, automated validation, transformation, and orchestration.

## Architecture Overview

### Components
- **Extraction Layer**: Custom adapters for APIs, databases, and file-based sources
- **Validation Engine**: Completeness, accuracy, and consistency checks
- **Transformation Engine**: Business rules, calculations, and data enrichment
- **Loading Layer**: Bulk operations to PostgreSQL data warehouse
- **Orchestration**: Apache Airflow for workflow management
- **Infrastructure**: Docker, Redis (locking), S3 (staging)

### Data Flow
1. Extract → S3 Staging → Validate → Transform → Load → PostgreSQL
2. Redis provides distributed locking for concurrent operations
3. Airflow orchestrates the entire pipeline with dependency management

## Quick Start

```bash
# Build and start all services
docker-compose up -d

# Access Airflow UI
http://localhost:8080 (admin/admin)

# Run ETL pipeline
docker-compose exec airflow-scheduler airflow dags trigger financial_etl_pipeline
```

## Project Structure

```
.
├── airflow/                 # Airflow DAGs and configuration
├── etl/                    # Core ETL components
│   ├── extractors/        # Data source adapters
│   ├── validators/        # Validation framework
│   ├── transformers/      # Transformation logic
│   └── loaders/          # Database loaders
├── config/                # Configuration files
├── docker/                # Docker configurations
├── tests/                 # Test suites
└── scripts/              # Utility scripts
```

## Supported Data Sources

1. REST APIs (Financial data providers)
2. PostgreSQL databases
3. MySQL databases
4. MongoDB collections
5. CSV files
6. JSON files
7. Parquet files
8. Excel files
9. XML files
10. SFTP servers
11. FTP servers
12. Google Sheets
13. Kafka streams
14. RabbitMQ queues
15. GraphQL APIs
16. WebSocket streams

## Key Features

### Extract (15+ Data Sources)
- **API Sources**: Alpha Vantage, Finnhub, Yahoo Finance, IEX Cloud, GraphQL
- **Databases**: PostgreSQL, MySQL, MongoDB
- **Files**: CSV, JSON, Parquet, Excel, XML
- **Streams**: Kafka, RabbitMQ, WebSocket
- **Transfer Protocols**: SFTP, FTP, Google Sheets

### Validate (3 Key Areas)
- **Completeness**: Null checks, required fields, row count validation
- **Accuracy**: Value ranges, regex patterns, categorical values, custom rules
- **Consistency**: Data types, uniqueness, date formats, cross-field rules

### Transform (Business Rules)
- **Stock Metrics**: Daily returns, moving averages (20/50/200-day), volatility, RSI
- **Financial Ratios**: Debt-to-equity, ROA, ROE, profit margin, current ratio
- **Portfolio Analytics**: Market value, unrealized P&L, position weights
- **Data Enrichment**: Lookups, derived columns, metadata addition

### Load (Bulk Operations)
- **Efficient Loading**: Bulk insert with configurable batch sizes (default: 10,000 records)
- **Upsert Support**: INSERT ... ON CONFLICT for idempotent loads
- **Distributed Locking**: Redis-based locks prevent concurrent write conflicts
- **Performance**: Optimized with connection pooling and execute_batch

## Architecture Details

### Data Flow
```
[Sources] → [Extract] → [S3 Staging] → [Validate] → [Transform] → [Load] → [PostgreSQL]
                ↓                          ↓              ↓           ↓
           [Metadata]                  [Quality Log]  [Business Rules]  [Analytics]
```

### Infrastructure Components
- **Airflow**: Orchestration with task dependencies and scheduling
- **PostgreSQL**: Two instances (data warehouse + Airflow metadata)
- **Redis**: Distributed locking for concurrent ETL operations
- **S3**: Intermediate staging area for data processing
- **Docker**: Containerized microservices for isolation and scalability

## Database Schema

### Schemas
- `raw`: Original extracted data
- `analytics`: Transformed business analytics
- `staging`: Temporary processing area
- `metadata`: ETL run logs and data quality metrics

### Key Tables
- `raw.stock_prices`: Daily stock price data
- `raw.company_financials`: Company financial statements
- `raw.economic_indicators`: Economic indicators
- `analytics.daily_stock_analytics`: Technical indicators and metrics
- `analytics.financial_ratios`: Calculated financial ratios
- `metadata.etl_run_log`: ETL execution tracking
- `metadata.data_quality_log`: Validation results

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Database
POSTGRES_HOST=postgres
POSTGRES_DB=financial_analytics
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=your_password

# Redis
REDIS_HOST=redis
REDIS_PASSWORD=your_redis_password

# AWS S3
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET=financial-etl-staging

# ETL Settings
ETL_BATCH_SIZE=10000
ETL_MAX_WORKERS=4
VALIDATION_STRICT_MODE=true
```

## Development Setup

```bash
# Setup development environment
./scripts/setup_dev.sh

# Activate virtual environment
source venv/bin/activate

# Run tests
pytest tests/ -v --cov=etl
```

## Deployment

```bash
# Deploy entire pipeline
./scripts/deploy.sh
```

This will:
1. Check prerequisites (Docker, Docker Compose)
2. Validate .env configuration
3. Build Docker images
4. Start all services
5. Initialize database schema
6. Verify service health

## Monitoring

### Airflow UI
- View DAG status and task execution
- Monitor ETL pipeline runs
- Check logs and error messages
- Trigger manual runs

### Database Metrics
```sql
-- Check ETL run statistics
SELECT dag_id, status, COUNT(*) as runs,
       AVG(records_loaded) as avg_records
FROM metadata.etl_run_log
GROUP BY dag_id, status;

-- Check data quality metrics
SELECT validation_type, 
       SUM(passed_records) as passed,
       SUM(failed_records) as failed
FROM metadata.data_quality_log
GROUP BY validation_type;
```

## Extending the Pipeline

### Adding New Data Sources
1. Create extractor in `etl/extractors/`
2. Add configuration in `config/data_sources.py`
3. Create DAG task in `airflow/dags/`
4. Configure validation rules

### Adding Custom Transformations
1. Extend `BaseTransformer` in `etl/transformers/`
2. Implement business logic in `transform()` method
3. Add to transformation pipeline in DAG

### Adding Validation Rules
1. Use existing validators with custom parameters
2. Or create custom validator extending `BaseValidator`
3. Configure in `config/data_sources.py`

## Performance Tuning

- **Batch Size**: Adjust `ETL_BATCH_SIZE` based on memory (default: 10,000)
- **Workers**: Increase `ETL_MAX_WORKERS` for parallel processing
- **Connection Pool**: Configure in `DatabaseConnection` (default: 10 connections)
- **S3 Format**: Use Parquet for optimal compression and speed
- **Indexes**: Add database indexes for frequently queried columns

## Troubleshooting

### Common Issues

**Airflow not starting**
```bash
# Check logs
docker-compose logs airflow-webserver

# Restart services
docker-compose restart airflow-webserver airflow-scheduler
```

**Database connection errors**
```bash
# Verify PostgreSQL is running
docker-compose ps postgres

# Test connection
docker-compose exec postgres psql -U etl_user -d financial_analytics
```

**S3 access issues**
- Verify AWS credentials in `.env`
- Check S3 bucket permissions
- Ensure bucket exists in specified region

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_validators.py -v

# Run with coverage
pytest tests/ --cov=etl --cov-report=html
```

## License

MIT License - See LICENSE file for details

## Support

For issues and questions:
- Check logs: `docker-compose logs -f [service_name]`
- Review Airflow UI for DAG execution details
- Examine metadata tables for ETL run history
