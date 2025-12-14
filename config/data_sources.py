"""
Data source configurations for ETL extractors
"""

# Financial data sources configuration
DATA_SOURCES = {
    # API Sources (5)
    'alpha_vantage': {
        'type': 'api',
        'extractor': 'AlphaVantageExtractor',
        'enabled': True,
        'description': 'Stock market data from Alpha Vantage',
        'rate_limit': '5_per_minute'
    },
    'finnhub': {
        'type': 'api',
        'extractor': 'FinnhubExtractor',
        'enabled': True,
        'description': 'Real-time financial data',
        'rate_limit': '60_per_minute'
    },
    'yahoo_finance': {
        'type': 'api',
        'extractor': 'YahooFinanceExtractor',
        'enabled': True,
        'description': 'Historical stock data',
        'rate_limit': 'unlimited'
    },
    'iex_cloud': {
        'type': 'api',
        'extractor': 'IEXCloudExtractor',
        'enabled': True,
        'description': 'Market data and financials',
        'rate_limit': '100_per_second'
    },
    'graphql_api': {
        'type': 'api',
        'extractor': 'GraphQLExtractor',
        'enabled': True,
        'description': 'Custom GraphQL financial API',
        'endpoint': 'https://api.example.com/graphql'
    },
    
    # Database Sources (3)
    'postgres_financial': {
        'type': 'database',
        'extractor': 'PostgreSQLExtractor',
        'enabled': True,
        'description': 'Legacy financial database',
        'connection': 'postgresql://user:pass@host:5432/findb'
    },
    'mysql_trading': {
        'type': 'database',
        'extractor': 'MySQLExtractor',
        'enabled': True,
        'description': 'Trading system database',
        'host': 'mysql-server',
        'database': 'trading_db'
    },
    'mongodb_analytics': {
        'type': 'database',
        'extractor': 'MongoDBExtractor',
        'enabled': True,
        'description': 'Analytics NoSQL database',
        'connection': 'mongodb://localhost:27017',
        'database': 'analytics'
    },
    
    # File Sources (5)
    'csv_files': {
        'type': 'file',
        'extractor': 'CSVExtractor',
        'enabled': True,
        'description': 'Daily CSV exports',
        'path_pattern': '/data/exports/*.csv'
    },
    'json_feeds': {
        'type': 'file',
        'extractor': 'JSONExtractor',
        'enabled': True,
        'description': 'JSON data feeds',
        'path_pattern': '/data/feeds/*.json'
    },
    'parquet_dumps': {
        'type': 'file',
        'extractor': 'ParquetExtractor',
        'enabled': True,
        'description': 'Parquet data dumps',
        'path_pattern': '/data/dumps/*.parquet'
    },
    'excel_reports': {
        'type': 'file',
        'extractor': 'ExcelExtractor',
        'enabled': True,
        'description': 'Financial Excel reports',
        'path_pattern': '/data/reports/*.xlsx'
    },
    'xml_feeds': {
        'type': 'file',
        'extractor': 'XMLExtractor',
        'enabled': True,
        'description': 'XML financial feeds',
        'path_pattern': '/data/xml/*.xml'
    },
    
    # Stream Sources (3)
    'kafka_trades': {
        'type': 'stream',
        'extractor': 'KafkaExtractor',
        'enabled': True,
        'description': 'Real-time trade data',
        'bootstrap_servers': ['kafka:9092'],
        'topic': 'trades'
    },
    'rabbitmq_quotes': {
        'type': 'stream',
        'extractor': 'RabbitMQExtractor',
        'enabled': True,
        'description': 'Market quotes queue',
        'host': 'rabbitmq',
        'queue': 'market_quotes'
    },
    'websocket_prices': {
        'type': 'stream',
        'extractor': 'WebSocketExtractor',
        'enabled': True,
        'description': 'Live price updates',
        'url': 'wss://stream.example.com/prices'
    },
    
    # Transfer Sources (3)
    'sftp_reports': {
        'type': 'transfer',
        'extractor': 'SFTPExtractor',
        'enabled': True,
        'description': 'SFTP financial reports',
        'host': 'sftp.example.com',
        'username': 'etl_user'
    },
    'ftp_data': {
        'type': 'transfer',
        'extractor': 'FTPExtractor',
        'enabled': True,
        'description': 'FTP data files',
        'host': 'ftp.example.com'
    },
    'google_sheets': {
        'type': 'transfer',
        'extractor': 'GoogleSheetsExtractor',
        'enabled': True,
        'description': 'Google Sheets data',
        'credentials_path': '/config/google_credentials.json'
    }
}

# Validation rules per data source
VALIDATION_RULES = {
    'stock_prices': {
        'completeness': {
            'required_columns': ['symbol', 'trade_date', 'close_price', 'volume'],
            'null_threshold': 5,
            'min_row_count': 1
        },
        'accuracy': {
            'value_ranges': {
                'open_price': (0, 100000),
                'high_price': (0, 100000),
                'low_price': (0, 100000),
                'close_price': (0, 100000),
                'volume': (0, 1e15)
            }
        },
        'consistency': {
            'data_types': {
                'symbol': 'object',
                'trade_date': 'datetime64[ns]',
                'close_price': 'float64',
                'volume': 'int64'
            }
        }
    },
    'company_financials': {
        'completeness': {
            'required_columns': ['symbol', 'fiscal_year', 'revenue', 'net_income'],
            'null_threshold': 10
        },
        'accuracy': {
            'value_ranges': {
                'revenue': (0, 1e15),
                'net_income': (-1e15, 1e15)
            }
        }
    }
}

# Transformation configurations
TRANSFORMATION_CONFIG = {
    'stock_prices': {
        'moving_average_windows': [20, 50, 200],
        'volatility_window': 20,
        'rsi_period': 14
    },
    'financial_ratios': {
        'ratios': ['debt_to_equity', 'current_ratio', 'quick_ratio', 'roa', 'roe', 'profit_margin']
    }
}

# Loading configurations
LOAD_CONFIG = {
    'stock_prices': {
        'target_table': 'raw.stock_prices',
        'if_exists': 'append',
        'use_lock': True,
        'batch_size': 10000
    },
    'stock_analytics': {
        'target_table': 'analytics.daily_stock_analytics',
        'if_exists': 'append',
        'use_lock': True,
        'batch_size': 10000
    },
    'company_financials': {
        'target_table': 'raw.company_financials',
        'if_exists': 'append',
        'use_lock': True
    },
    'financial_ratios': {
        'target_table': 'analytics.financial_ratios',
        'if_exists': 'append',
        'use_lock': True
    }
}
