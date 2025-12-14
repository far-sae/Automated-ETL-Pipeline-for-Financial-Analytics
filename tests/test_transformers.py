"""
Unit tests for transformers
"""
import pytest
import pandas as pd
import numpy as np
from etl.transformers import StockPriceTransformer, FinancialRatioTransformer


class TestStockPriceTransformer:
    """Test stock price transformation"""
    
    def test_transform_basic(self):
        """Test basic stock price transformation"""
        df = pd.DataFrame({
            'symbol': ['AAPL'] * 30,
            'trade_date': pd.date_range('2024-01-01', periods=30),
            'close_price': np.random.uniform(140, 160, 30),
            'open_price': np.random.uniform(140, 160, 30),
            'high_price': np.random.uniform(140, 160, 30),
            'low_price': np.random.uniform(140, 160, 30),
            'volume': np.random.randint(1000000, 2000000, 30)
        })
        
        transformer = StockPriceTransformer()
        result_df = transformer.execute(df)
        
        # Check that calculated columns exist
        assert 'daily_return' in result_df.columns
        assert 'moving_avg_20d' in result_df.columns
        assert 'moving_avg_50d' in result_df.columns
        assert 'moving_avg_200d' in result_df.columns
        assert 'volatility_20d' in result_df.columns
        assert 'rsi_14d' in result_df.columns
    
    def test_transform_multiple_symbols(self):
        """Test transformation with multiple symbols"""
        symbols = ['AAPL', 'GOOGL', 'MSFT']
        df_list = []
        
        for symbol in symbols:
            df_symbol = pd.DataFrame({
                'symbol': [symbol] * 50,
                'trade_date': pd.date_range('2024-01-01', periods=50),
                'close_price': np.random.uniform(100, 200, 50),
                'open_price': np.random.uniform(100, 200, 50),
                'high_price': np.random.uniform(100, 200, 50),
                'low_price': np.random.uniform(100, 200, 50),
                'volume': np.random.randint(1000000, 5000000, 50)
            })
            df_list.append(df_symbol)
        
        df = pd.concat(df_list, ignore_index=True)
        
        transformer = StockPriceTransformer()
        result_df = transformer.execute(df)
        
        # Check that all symbols are present
        assert set(result_df['symbol'].unique()) == set(symbols)
        
        # Check calculations are done per symbol
        for symbol in symbols:
            symbol_data = result_df[result_df['symbol'] == symbol]
            assert not symbol_data['daily_return'].isna().all()


class TestFinancialRatioTransformer:
    """Test financial ratio transformation"""
    
    def test_calculate_ratios(self):
        """Test financial ratio calculations"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'fiscal_year': [2023, 2023],
            'revenue': [400000000000, 300000000000],
            'net_income': [100000000000, 75000000000],
            'total_assets': [350000000000, 400000000000],
            'total_liabilities': [150000000000, 100000000000],
            'shareholders_equity': [200000000000, 300000000000]
        })
        
        transformer = FinancialRatioTransformer()
        result_df = transformer.execute(df)
        
        # Check that ratio columns exist
        assert 'debt_to_equity' in result_df.columns
        assert 'roa' in result_df.columns
        assert 'roe' in result_df.columns
        assert 'profit_margin' in result_df.columns
        assert 'asset_turnover' in result_df.columns
        
        # Basic sanity checks
        assert (result_df['debt_to_equity'] >= 0).all()
        assert (result_df['asset_turnover'] >= 0).all()
