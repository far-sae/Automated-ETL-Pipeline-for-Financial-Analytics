"""
Accuracy validation - verifies data correctness against defined standards
"""
from typing import Dict, Any, List, Optional, Callable
import pandas as pd
import numpy as np
from etl.validators.base import BaseValidator, ValidationResult
from etl.logger import get_logger

logger = get_logger(__name__)


class AccuracyValidator(BaseValidator):
    """Validate data accuracy"""
    
    def __init__(self, strict_mode: bool = False):
        super().__init__("accuracy_validator", strict_mode)
    
    def validate(self, df: pd.DataFrame, **kwargs) -> ValidationResult:
        """
        Validate data accuracy using various checks
        
        Kwargs:
            value_ranges: Dict[column_name, (min, max)] for numeric ranges
            categorical_values: Dict[column_name, List[valid_values]]
            regex_patterns: Dict[column_name, regex_pattern]
            custom_rules: Dict[column_name, Callable[[pd.Series], pd.Series]]
        """
        value_ranges = kwargs.get('value_ranges', {})
        categorical_values = kwargs.get('categorical_values', {})
        regex_patterns = kwargs.get('regex_patterns', {})
        custom_rules = kwargs.get('custom_rules', {})
        
        error_details = []
        passed_count = 0
        failed_count = 0
        
        # Validate numeric ranges
        for column, (min_val, max_val) in value_ranges.items():
            if column in df.columns:
                mask = (df[column] < min_val) | (df[column] > max_val)
                violations = df[mask]
                
                if len(violations) > 0:
                    failed_count += len(violations)
                    error_details.append({
                        'check': 'value_range',
                        'column': column,
                        'range': [min_val, max_val],
                        'violations': len(violations),
                        'sample_values': violations[column].head(5).tolist()
                    })
                
                passed_count += len(df) - len(violations)
        
        # Validate categorical values
        for column, valid_values in categorical_values.items():
            if column in df.columns:
                mask = ~df[column].isin(valid_values)
                violations = df[mask]
                
                if len(violations) > 0:
                    failed_count += len(violations)
                    error_details.append({
                        'check': 'categorical_values',
                        'column': column,
                        'valid_values': valid_values[:10],
                        'violations': len(violations),
                        'invalid_values': violations[column].unique().tolist()[:10]
                    })
                
                passed_count += len(df) - len(violations)
        
        # Validate regex patterns
        for column, pattern in regex_patterns.items():
            if column in df.columns:
                mask = ~df[column].astype(str).str.match(pattern, na=False)
                violations = df[mask]
                
                if len(violations) > 0:
                    failed_count += len(violations)
                    error_details.append({
                        'check': 'regex_pattern',
                        'column': column,
                        'pattern': pattern,
                        'violations': len(violations),
                        'sample_values': violations[column].head(5).tolist()
                    })
                
                passed_count += len(df) - len(violations)
        
        # Apply custom validation rules
        for column, rule_func in custom_rules.items():
            if column in df.columns:
                try:
                    mask = rule_func(df[column])
                    violations = df[~mask]
                    
                    if len(violations) > 0:
                        failed_count += len(violations)
                        error_details.append({
                            'check': 'custom_rule',
                            'column': column,
                            'rule': rule_func.__name__,
                            'violations': len(violations)
                        })
                    
                    passed_count += len(df) - len(violations)
                except Exception as e:
                    logger.error("custom_rule_error", column=column, error=str(e))
        
        total_records = passed_count + failed_count
        passed = len(error_details) == 0
        
        result = ValidationResult(
            validator_name=self.name,
            validation_type="accuracy",
            passed=passed,
            passed_records=passed_count,
            failed_records=failed_count,
            total_records=total_records,
            error_details=error_details,
            metadata={
                'checks_performed': len(value_ranges) + len(categorical_values) + len(regex_patterns) + len(custom_rules)
            }
        )
        
        self.log_result(result)
        self.raise_if_failed(result)
        
        return result
