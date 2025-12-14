"""
Financial data transformers with business rules and calculations
"""
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
from etl.transformers.base import BaseTransformer
from etl.logger import get_logger

logger = get_logger(__name__)


class StockPriceTransformer(BaseTransformer):
    """Transform raw stock price data with technical indicators"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("stock_price_transformer", config)
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform stock price data
        
        Adds: daily returns, moving averages, volatility, RSI
        """
        df = df.copy()
        
        # Ensure proper date ordering
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values(['symbol', 'trade_date'])
        
        # Calculate daily returns
        df['daily_return'] = df.groupby('symbol')['close_price'].pct_change()
        
        # Calculate moving averages
        for window in [20, 50, 200]:
            df[f'moving_avg_{window}d'] = df.groupby('symbol')['close_price'].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean()
            )
        
        # Calculate volatility (20-day rolling std of returns)
        df['volatility_20d'] = df.groupby('symbol')['daily_return'].transform(
            lambda x: x.rolling(window=20, min_periods=1).std()
        )
        
        # Calculate RSI (14-day)
        df['rsi_14d'] = df.groupby('symbol').apply(
            lambda x: self._calculate_rsi(x['close_price'], 14)
        ).reset_index(level=0, drop=True)
        
        return df
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi


class FinancialRatioTransformer(BaseTransformer):
    """Calculate financial ratios from company financials"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("financial_ratio_transformer", config)
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Calculate financial ratios
        
        Includes: debt-to-equity, current ratio, quick ratio, ROA, ROE, profit margin
        """
        df = df.copy()
        
        # Debt-to-Equity Ratio
        df['debt_to_equity'] = df['total_liabilities'] / df['shareholders_equity'].replace(0, np.nan)
        
        # Current Ratio (assumed current assets = total_assets * 0.4 for demo)
        df['current_ratio'] = (df['total_assets'] * 0.4) / (df['total_liabilities'] * 0.3).replace(0, np.nan)
        
        # Quick Ratio
        df['quick_ratio'] = (df['total_assets'] * 0.3) / (df['total_liabilities'] * 0.3).replace(0, np.nan)
        
        # Return on Assets (ROA)
        df['roa'] = (df['net_income'] / df['total_assets'].replace(0, np.nan)) * 100
        
        # Return on Equity (ROE)
        df['roe'] = (df['net_income'] / df['shareholders_equity'].replace(0, np.nan)) * 100
        
        # Profit Margin
        df['profit_margin'] = (df['net_income'] / df['revenue'].replace(0, np.nan)) * 100
        
        # Asset Turnover
        df['asset_turnover'] = df['revenue'] / df['total_assets'].replace(0, np.nan)
        
        # P/E Ratio (placeholder - would need stock price data)
        # df['pe_ratio'] = calculated from market cap / earnings
        
        # P/B Ratio
        # df['pb_ratio'] = calculated from market cap / book value
        
        return df


class PortfolioTransformer(BaseTransformer):
    """Transform portfolio position data"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("portfolio_transformer", config)
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform portfolio data
        
        Calculates: market value, unrealized P&L, position weights
        """
        df = df.copy()
        
        # Calculate market value
        df['market_value'] = df['quantity'] * df['current_price']
        
        # Calculate unrealized P&L
        df['unrealized_pnl'] = (df['current_price'] - df['avg_cost']) * df['quantity']
        
        # Calculate position weights within each portfolio
        df['weight'] = df.groupby(['portfolio_id', 'position_date'])['market_value'].transform(
            lambda x: x / x.sum()
        )
        
        # Add performance metrics
        df['pnl_percentage'] = (df['unrealized_pnl'] / (df['avg_cost'] * df['quantity']).replace(0, np.nan)) * 100
        
        return df
