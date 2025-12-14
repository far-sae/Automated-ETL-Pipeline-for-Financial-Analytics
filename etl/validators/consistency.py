"""
Consistency validation - maintains uniform data formats and values across sources
"""
from typing import Dict, Any, List, Optional
import pandas as pd
from etl.validators.base import BaseValidator, ValidationResult
from etl.logger import get_logger

logger = get_logger(__name__)


class ConsistencyValidator(BaseValidator):
    """Validate data consistency"""
    
    def __init__(self, strict_mode: bool = False):
        super().__init__("consistency_validator", strict_mode)
    
    def validate(self, df: pd.DataFrame, **kwargs) -> ValidationResult:
        """
        Validate data consistency
        
        Kwargs:
            data_types: Dict[column_name, expected_dtype]
            unique_columns: List of columns that should be unique
            date_formats: Dict[column_name, expected_date_format]
            cross_field_rules: List of (column1, column2, comparison_func)
        """
        data_types = kwargs.get('data_types', {})
        unique_columns = kwargs.get('unique_columns', [])
        date_formats = kwargs.get('date_formats', {})
        cross_field_rules = kwargs.get('cross_field_rules', [])
        
        error_details = []
        passed_count = 0
        failed_count = 0
        
        # Validate data types
        for column, expected_dtype in data_types.items():
            if column in df.columns:
                actual_dtype = df[column].dtype
                if str(actual_dtype) != expected_dtype:
                    error_details.append({
                        'check': 'data_type',
                        'column': column,
                        'expected': expected_dtype,
                        'actual': str(actual_dtype),
                        'message': f"Data type mismatch for {column}"
                    })
                    failed_count += len(df)
                else:
                    passed_count += len(df)
        
        # Validate uniqueness
        for column in unique_columns:
            if column in df.columns:
                duplicates = df[column].duplicated().sum()
                if duplicates > 0:
                    error_details.append({
                        'check': 'uniqueness',
                        'column': column,
                        'duplicates': int(duplicates),
                        'message': f"{duplicates} duplicate values found in {column}"
                    })
                    failed_count += duplicates
                    passed_count += len(df) - duplicates
                else:
                    passed_count += len(df)
        
        # Validate date formats
        for column, date_format in date_formats.items():
            if column in df.columns:
                try:
                    pd.to_datetime(df[column], format=date_format, errors='raise')
                    passed_count += len(df)
                except:
                    invalid_dates = 0
                    for val in df[column]:
                        try:
                            pd.to_datetime(val, format=date_format)
                        except:
                            invalid_dates += 1
                    
                    if invalid_dates > 0:
                        error_details.append({
                            'check': 'date_format',
                            'column': column,
                            'expected_format': date_format,
                            'invalid_count': invalid_dates,
                            'message': f"{invalid_dates} values don't match date format"
                        })
                        failed_count += invalid_dates
                        passed_count += len(df) - invalid_dates
        
        # Validate cross-field rules
        for col1, col2, comparison_func in cross_field_rules:
            if col1 in df.columns and col2 in df.columns:
                try:
                    mask = comparison_func(df[col1], df[col2])
                    violations = (~mask).sum()
                    
                    if violations > 0:
                        error_details.append({
                            'check': 'cross_field',
                            'columns': [col1, col2],
                            'violations': int(violations),
                            'message': f"Cross-field validation failed for {col1} and {col2}"
                        })
                        failed_count += violations
                        passed_count += len(df) - violations
                    else:
                        passed_count += len(df)
                except Exception as e:
                    logger.error("cross_field_validation_error", columns=[col1, col2], error=str(e))
        
        total_records = passed_count + failed_count
        passed = len(error_details) == 0
        
        result = ValidationResult(
            validator_name=self.name,
            validation_type="consistency",
            passed=passed,
            passed_records=passed_count,
            failed_records=failed_count,
            total_records=total_records,
            error_details=error_details,
            metadata={
                'data_type_checks': len(data_types),
                'uniqueness_checks': len(unique_columns),
                'date_format_checks': len(date_formats),
                'cross_field_checks': len(cross_field_rules)
            }
        )
        
        self.log_result(result)
        self.raise_if_failed(result)
        
        return result
