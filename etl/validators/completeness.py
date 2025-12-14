"""
Completeness validation - ensures all expected data is present
"""
from typing import Dict, Any, List, Optional
import pandas as pd
from etl.validators.base import BaseValidator, ValidationResult
from etl.logger import get_logger

logger = get_logger(__name__)


class CompletenessValidator(BaseValidator):
    """Validate data completeness"""
    
    def __init__(self, strict_mode: bool = False):
        super().__init__("completeness_validator", strict_mode)
    
    def validate(self, df: pd.DataFrame, **kwargs) -> ValidationResult:
        """
        Validate data completeness
        
        Kwargs:
            required_columns: List of required column names
            null_threshold: Maximum allowed null percentage per column (0-100)
            min_row_count: Minimum expected number of rows
        """
        required_columns = kwargs.get('required_columns', [])
        null_threshold = kwargs.get('null_threshold', 0)
        min_row_count = kwargs.get('min_row_count', 1)
        
        error_details = []
        passed = True
        
        # Check minimum row count
        if len(df) < min_row_count:
            error_details.append({
                'check': 'min_row_count',
                'expected': min_row_count,
                'actual': len(df),
                'message': f"Row count {len(df)} below minimum {min_row_count}"
            })
            passed = False
        
        # Check required columns exist
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            error_details.append({
                'check': 'required_columns',
                'missing_columns': list(missing_columns),
                'message': f"Missing required columns: {missing_columns}"
            })
            passed = False
        
        # Check null values in each column
        null_violations = {}
        for column in df.columns:
            null_count = df[column].isnull().sum()
            null_percentage = (null_count / len(df)) * 100 if len(df) > 0 else 0
            
            if null_percentage > null_threshold:
                null_violations[column] = {
                    'null_count': int(null_count),
                    'null_percentage': round(null_percentage, 2),
                    'threshold': null_threshold
                }
        
        if null_violations:
            error_details.append({
                'check': 'null_threshold',
                'violations': null_violations,
                'message': f"{len(null_violations)} columns exceed null threshold"
            })
            passed = False
        
        # Calculate statistics
        total_cells = df.shape[0] * df.shape[1] if df.shape[1] > 0 else 0
        null_cells = df.isnull().sum().sum()
        complete_cells = total_cells - null_cells
        
        result = ValidationResult(
            validator_name=self.name,
            validation_type="completeness",
            passed=passed,
            passed_records=int(complete_cells),
            failed_records=int(null_cells),
            total_records=int(total_cells),
            error_details=error_details,
            metadata={
                'row_count': len(df),
                'column_count': len(df.columns),
                'completeness_rate': round((complete_cells / total_cells * 100) if total_cells > 0 else 0, 2)
            }
        )
        
        self.log_result(result)
        self.raise_if_failed(result)
        
        return result
    
    def validate_expected_records(self, df: pd.DataFrame, expected_count: int,
                                  tolerance: float = 0.1) -> ValidationResult:
        """
        Validate expected record count with tolerance
        
        Args:
            df: DataFrame to validate
            expected_count: Expected number of records
            tolerance: Acceptable deviation (0.1 = 10%)
        """
        actual_count = len(df)
        min_expected = expected_count * (1 - tolerance)
        max_expected = expected_count * (1 + tolerance)
        
        passed = min_expected <= actual_count <= max_expected
        error_details = []
        
        if not passed:
            error_details.append({
                'check': 'expected_record_count',
                'expected': expected_count,
                'actual': actual_count,
                'tolerance': tolerance,
                'range': [int(min_expected), int(max_expected)],
                'message': f"Record count {actual_count} outside expected range [{int(min_expected)}, {int(max_expected)}]"
            })
        
        result = ValidationResult(
            validator_name=self.name,
            validation_type="expected_record_count",
            passed=passed,
            passed_records=actual_count if passed else 0,
            failed_records=0 if passed else actual_count,
            total_records=actual_count,
            error_details=error_details,
            metadata={
                'expected_count': expected_count,
                'actual_count': actual_count,
                'tolerance': tolerance,
                'deviation': round(((actual_count - expected_count) / expected_count * 100) if expected_count > 0 else 0, 2)
            }
        )
        
        self.log_result(result)
        self.raise_if_failed(result)
        
        return result
