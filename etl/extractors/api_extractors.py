"""
API-based data extractors for financial data sources
"""
from typing import Dict, Any, List, Optional
import time
import requests
from datetime import datetime, timedelta
import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from etl.extractors.base import BaseExtractor
from etl.config import config
from etl.logger import get_logger

logger = get_logger(__name__)


class AlphaVantageExtractor(BaseExtractor):
    """Extract stock market data from Alpha Vantage API"""
    
    BASE_URL = "https://www.alphavantage.co/query"
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("alpha_vantage", config_dict)
        credentials = config.get_api_credentials('alpha_vantage')
        self.api_key = credentials.get('api_key', '')
    
    def extract(self, symbols: List[str], function: str = "TIME_SERIES_DAILY", **kwargs) -> pd.DataFrame:
        """
        Extract stock data from Alpha Vantage
        
        Args:
            symbols: List of stock symbols
            function: API function to call
            
        Returns:
            DataFrame with stock data
        """
        all_data = []
        
        for symbol in symbols:
            try:
                params = {
                    'function': function,
                    'symbol': symbol,
                    'apikey': self.api_key,
                    'outputsize': kwargs.get('outputsize', 'compact')
                }
                
                response = requests.get(self.BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()
                
                # Parse time series data
                if 'Time Series (Daily)' in data:
                    time_series = data['Time Series (Daily)']
                    for date_str, values in time_series.items():
                        all_data.append({
                            'symbol': symbol,
                            'trade_date': date_str,
                            'open_price': float(values['1. open']),
                            'high_price': float(values['2. high']),
                            'low_price': float(values['3. low']),
                            'close_price': float(values['4. close']),
                            'volume': int(values['5. volume'])
                        })
                
                # Rate limiting (5 calls per minute for free tier)
                time.sleep(12)
                
            except Exception as e:
                logger.error("alpha_vantage_extraction_failed", symbol=symbol, error=str(e))
        
        return pd.DataFrame(all_data)


class FinnhubExtractor(BaseExtractor):
    """Extract financial data from Finnhub API"""
    
    BASE_URL = "https://finnhub.io/api/v1"
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("finnhub", config_dict)
        credentials = config.get_api_credentials('finnhub')
        self.api_key = credentials.get('api_key', '')
    
    def extract(self, symbols: List[str], endpoint: str = "quote", **kwargs) -> pd.DataFrame:
        """Extract data from Finnhub"""
        all_data = []
        
        for symbol in symbols:
            try:
                url = f"{self.BASE_URL}/{endpoint}"
                params = {'symbol': symbol, 'token': self.api_key}
                
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                data['symbol'] = symbol
                data['timestamp'] = datetime.now().isoformat()
                all_data.append(data)
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error("finnhub_extraction_failed", symbol=symbol, error=str(e))
        
        return pd.DataFrame(all_data)


class YahooFinanceExtractor(BaseExtractor):
    """Extract financial data from Yahoo Finance"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("yahoo_finance", config_dict)
    
    def extract(self, symbols: List[str], period: str = "1mo", interval: str = "1d") -> pd.DataFrame:
        """
        Extract stock data from Yahoo Finance
        
        Args:
            symbols: List of stock symbols
            period: Data period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        """
        import yfinance as yf
        
        all_data = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period=period, interval=interval)
                
                if not hist.empty:
                    hist.reset_index(inplace=True)
                    hist['symbol'] = symbol
                    hist.columns = [col.lower().replace(' ', '_') for col in hist.columns]
                    all_data.append(hist)
                
            except Exception as e:
                logger.error("yahoo_finance_extraction_failed", symbol=symbol, error=str(e))
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()


class IEXCloudExtractor(BaseExtractor):
    """Extract financial data from IEX Cloud API"""
    
    BASE_URL = "https://cloud.iexapis.com/stable"
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("iex_cloud", config_dict)
        credentials = config.get_api_credentials('iex_cloud')
        self.api_key = credentials.get('api_key', '')
    
    def extract(self, symbols: List[str], endpoint: str = "quote", **kwargs) -> pd.DataFrame:
        """Extract data from IEX Cloud"""
        all_data = []
        
        for symbol in symbols:
            try:
                url = f"{self.BASE_URL}/stock/{symbol}/{endpoint}"
                params = {'token': self.api_key}
                
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, dict):
                    data['symbol'] = symbol
                    all_data.append(data)
                elif isinstance(data, list):
                    for item in data:
                        item['symbol'] = symbol
                        all_data.append(item)
                
            except Exception as e:
                logger.error("iex_cloud_extraction_failed", symbol=symbol, error=str(e))
        
        return pd.DataFrame(all_data)


class GraphQLExtractor(BaseExtractor):
    """Extract data from GraphQL APIs"""
    
    def __init__(self, endpoint: str, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("graphql_api", config_dict)
        self.endpoint = endpoint
        
        transport = RequestsHTTPTransport(
            url=endpoint,
            headers=config_dict.get('headers', {}) if config_dict else {},
            verify=True,
            retries=3
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=True)
    
    def extract(self, query_string: str, variables: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Extract data using GraphQL query
        
        Args:
            query_string: GraphQL query string
            variables: Query variables
        """
        try:
            query = gql(query_string)
            result = self.client.execute(query, variable_values=variables)
            
            # Flatten nested JSON structure
            if isinstance(result, dict):
                # Extract the first data array found
                for key, value in result.items():
                    if isinstance(value, list):
                        return pd.json_normalize(value)
            
            return pd.json_normalize(result)
            
        except Exception as e:
            logger.error("graphql_extraction_failed", error=str(e))
            return pd.DataFrame()
