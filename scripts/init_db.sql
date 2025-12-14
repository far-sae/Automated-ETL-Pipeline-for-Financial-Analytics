-- Initialize Financial Analytics Data Warehouse
-- This script creates the schema and tables for the ETL pipeline

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS metadata;

-- Metadata tables for ETL tracking
CREATE TABLE IF NOT EXISTS metadata.etl_run_log (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    run_start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    records_extracted INTEGER,
    records_validated INTEGER,
    records_loaded INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata.data_quality_log (
    quality_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES metadata.etl_run_log(run_id),
    validation_type VARCHAR(100) NOT NULL,
    validation_rule VARCHAR(255) NOT NULL,
    passed_records INTEGER,
    failed_records INTEGER,
    validation_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw tables for financial data
CREATE TABLE IF NOT EXISTS raw.stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(18, 4),
    high_price DECIMAL(18, 4),
    low_price DECIMAL(18, 4),
    close_price DECIMAL(18, 4),
    adjusted_close DECIMAL(18, 4),
    volume BIGINT,
    source_system VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_date, source_system)
);

CREATE TABLE IF NOT EXISTS raw.company_financials (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    fiscal_period VARCHAR(20) NOT NULL,
    fiscal_year INTEGER NOT NULL,
    revenue DECIMAL(20, 2),
    net_income DECIMAL(20, 2),
    total_assets DECIMAL(20, 2),
    total_liabilities DECIMAL(20, 2),
    shareholders_equity DECIMAL(20, 2),
    cash_flow_operating DECIMAL(20, 2),
    cash_flow_investing DECIMAL(20, 2),
    cash_flow_financing DECIMAL(20, 2),
    source_system VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, fiscal_period, fiscal_year, source_system)
);

CREATE TABLE IF NOT EXISTS raw.economic_indicators (
    id SERIAL PRIMARY KEY,
    indicator_name VARCHAR(100) NOT NULL,
    indicator_date DATE NOT NULL,
    value DECIMAL(18, 6),
    unit VARCHAR(50),
    country VARCHAR(3),
    source_system VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(indicator_name, indicator_date, country, source_system)
);

CREATE TABLE IF NOT EXISTS raw.market_news (
    id SERIAL PRIMARY KEY,
    news_id VARCHAR(255) UNIQUE,
    headline TEXT NOT NULL,
    summary TEXT,
    content TEXT,
    published_at TIMESTAMP,
    source VARCHAR(255),
    symbols VARCHAR(500),
    sentiment_score DECIMAL(5, 4),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.cryptocurrency_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    trade_timestamp TIMESTAMP NOT NULL,
    price_usd DECIMAL(18, 8),
    volume_24h DECIMAL(20, 2),
    market_cap DECIMAL(20, 2),
    percent_change_24h DECIMAL(8, 4),
    source_system VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_timestamp, source_system)
);

-- Analytics tables (transformed data)
CREATE TABLE IF NOT EXISTS analytics.daily_stock_analytics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(18, 4),
    high_price DECIMAL(18, 4),
    low_price DECIMAL(18, 4),
    close_price DECIMAL(18, 4),
    volume BIGINT,
    daily_return DECIMAL(10, 6),
    volatility_20d DECIMAL(10, 6),
    moving_avg_20d DECIMAL(18, 4),
    moving_avg_50d DECIMAL(18, 4),
    moving_avg_200d DECIMAL(18, 4),
    rsi_14d DECIMAL(6, 2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS analytics.portfolio_positions (
    id SERIAL PRIMARY KEY,
    portfolio_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    position_date DATE NOT NULL,
    quantity DECIMAL(18, 4),
    avg_cost DECIMAL(18, 4),
    current_price DECIMAL(18, 4),
    market_value DECIMAL(20, 2),
    unrealized_pnl DECIMAL(20, 2),
    weight DECIMAL(6, 4),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(portfolio_id, symbol, position_date)
);

CREATE TABLE IF NOT EXISTS analytics.financial_ratios (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    fiscal_period VARCHAR(20) NOT NULL,
    fiscal_year INTEGER NOT NULL,
    pe_ratio DECIMAL(10, 2),
    pb_ratio DECIMAL(10, 2),
    debt_to_equity DECIMAL(10, 4),
    current_ratio DECIMAL(10, 4),
    quick_ratio DECIMAL(10, 4),
    roa DECIMAL(8, 4),
    roe DECIMAL(8, 4),
    profit_margin DECIMAL(8, 4),
    asset_turnover DECIMAL(8, 4),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, fiscal_period, fiscal_year)
);

-- Create indexes for performance
CREATE INDEX idx_stock_prices_symbol_date ON raw.stock_prices(symbol, trade_date);
CREATE INDEX idx_company_financials_symbol ON raw.company_financials(symbol, fiscal_year);
CREATE INDEX idx_economic_indicators_name_date ON raw.economic_indicators(indicator_name, indicator_date);
CREATE INDEX idx_market_news_published ON raw.market_news(published_at);
CREATE INDEX idx_crypto_prices_symbol_ts ON raw.cryptocurrency_prices(symbol, trade_timestamp);
CREATE INDEX idx_daily_analytics_symbol_date ON analytics.daily_stock_analytics(symbol, trade_date);
CREATE INDEX idx_etl_run_log_status ON metadata.etl_run_log(status, run_start_time);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA analytics TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata TO etl_user;

-- Insert initial metadata
INSERT INTO metadata.etl_run_log (dag_id, task_id, source_name, status, run_start_time)
VALUES ('init', 'database_setup', 'system', 'SUCCESS', CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;
