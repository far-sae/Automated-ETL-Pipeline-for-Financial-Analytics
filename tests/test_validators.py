"""
Unit tests for validators
"""
import pytest
import pandas as pd
from datetime import datetime
from etl.validators import CompletenessValidator, AccuracyValidator, ConsistencyValidator


class TestCompletenessValidator:
    """Test completeness validation"""
    
    def test_validate_complete_data(self):
        """Test validation with complete data"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT'],
            'price': [150.0, 2800.0, 300.0],
            'volume': [1000000, 500000, 800000]
        })
        
        validator = CompletenessValidator(strict_mode=False)
        result = validator.validate(
            df,
            required_columns=['symbol', 'price', 'volume'],
            null_threshold=0,
            min_row_count=1
        )
        
        assert result.passed is True
        assert result.passed_records > 0
    
    def test_validate_missing_columns(self):
        """Test validation with missing required columns"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'price': [150.0, 2800.0]
        })
        
        validator = CompletenessValidator(strict_mode=False)
        result = validator.validate(
            df,
            required_columns=['symbol', 'price', 'volume'],
            null_threshold=0
        )
        
        assert result.passed is False
        assert any('missing_columns' in str(err) for err in result.error_details)
    
    def test_validate_null_values(self):
        """Test validation with null values exceeding threshold"""
        df = pd.DataFrame({
            'symbol': ['AAPL', None, 'MSFT'],
            'price': [150.0, 2800.0, 300.0]
        })
        
        validator = CompletenessValidator(strict_mode=False)
        result = validator.validate(
            df,
            required_columns=['symbol', 'price'],
            null_threshold=10  # 10% threshold
        )
        
        assert result.passed is False


class TestAccuracyValidator:
    """Test accuracy validation"""
    
    def test_validate_value_ranges(self):
        """Test validation of value ranges"""
        df = pd.DataFrame({
            'price': [150.0, 200.0, 180.0],
            'volume': [1000, 2000, 1500]
        })
        
        validator = AccuracyValidator(strict_mode=False)
        result = validator.validate(
            df,
            value_ranges={
                'price': (0, 300),
                'volume': (0, 10000)
            }
        )
        
        assert result.passed is True
    
    def test_validate_out_of_range(self):
        """Test validation with out-of-range values"""
        df = pd.DataFrame({
            'price': [150.0, 500.0, 180.0],  # 500 is out of range
            'volume': [1000, 2000, 1500]
        })
        
        validator = AccuracyValidator(strict_mode=False)
        result = validator.validate(
            df,
            value_ranges={
                'price': (0, 300),
                'volume': (0, 10000)
            }
        )
        
        assert result.passed is False
        assert result.failed_records > 0


class TestConsistencyValidator:
    """Test consistency validation"""
    
    def test_validate_data_types(self):
        """Test data type validation"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL'],
            'price': [150.0, 2800.0],
            'volume': [1000000, 500000]
        })
        
        validator = ConsistencyValidator(strict_mode=False)
        result = validator.validate(
            df,
            data_types={
                'symbol': 'object',
                'price': 'float64',
                'volume': 'int64'
            }
        )
        
        assert result.passed is True
    
    def test_validate_uniqueness(self):
        """Test uniqueness validation"""
        df = pd.DataFrame({
            'id': [1, 2, 2, 3],  # Duplicate ID
            'value': [100, 200, 300, 400]
        })
        
        validator = ConsistencyValidator(strict_mode=False)
        result = validator.validate(
            df,
            unique_columns=['id']
        )
        
        assert result.passed is False
        assert result.failed_records > 0
