"""
Schema validation - validates DataFrame schema
"""
from typing import Dict, Any, List, Optional
import pandas as pd
from pydantic import BaseModel, create_model, ValidationError
from etl.validators.base import BaseValidator, ValidationResult
from etl.logger import get_logger

logger = get_logger(__name__)


class SchemaValidator(BaseValidator):
    """Validate DataFrame schema"""
    
    def __init__(self, strict_mode: bool = False):
        super().__init__("schema_validator", strict_mode)
    
    def validate(self, df: pd.DataFrame, **kwargs) -> ValidationResult:
        """
        Validate DataFrame against expected schema
        
        Kwargs:
            expected_schema: Dict[column_name, {type, nullable, description}]
            allow_extra_columns: If True, allow columns not in schema
        """
        expected_schema = kwargs.get('expected_schema', {})
        allow_extra_columns = kwargs.get('allow_extra_columns', True)
        
        error_details = []
        passed = True
        
        # Check for missing columns
        expected_columns = set(expected_schema.keys())
        actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            error_details.append({
                'check': 'missing_columns',
                'columns': list(missing_columns),
                'message': f"Missing expected columns: {missing_columns}"
            })
            passed = False
        
        # Check for extra columns
        if not allow_extra_columns:
            extra_columns = actual_columns - expected_columns
            if extra_columns:
                error_details.append({
                    'check': 'extra_columns',
                    'columns': list(extra_columns),
                    'message': f"Unexpected columns found: {extra_columns}"
                })
                passed = False
        
        # Validate column properties
        for column, schema_def in expected_schema.items():
            if column not in df.columns:
                continue
            
            expected_type = schema_def.get('type')
            nullable = schema_def.get('nullable', True)
            
            # Check nullability
            if not nullable and df[column].isnull().any():
                null_count = df[column].isnull().sum()
                error_details.append({
                    'check': 'nullability',
                    'column': column,
                    'null_count': int(null_count),
                    'message': f"Column {column} should not contain nulls but has {null_count}"
                })
                passed = False
            
            # Check data type compatibility
            if expected_type:
                actual_type = str(df[column].dtype)
                if not self._is_type_compatible(actual_type, expected_type):
                    error_details.append({
                        'check': 'data_type',
                        'column': column,
                        'expected': expected_type,
                        'actual': actual_type,
                        'message': f"Type mismatch for {column}"
                    })
                    passed = False
        
        result = ValidationResult(
            validator_name=self.name,
            validation_type="schema",
            passed=passed,
            passed_records=len(df) if passed else 0,
            failed_records=0 if passed else len(df),
            total_records=len(df),
            error_details=error_details,
            metadata={
                'expected_columns': len(expected_schema),
                'actual_columns': len(df.columns),
                'allow_extra_columns': allow_extra_columns
            }
        )
        
        self.log_result(result)
        self.raise_if_failed(result)
        
        return result
    
    def _is_type_compatible(self, actual_type: str, expected_type: str) -> bool:
        """Check if actual and expected types are compatible"""
        type_mappings = {
            'int': ['int64', 'int32', 'int16', 'int8'],
            'float': ['float64', 'float32', 'float16'],
            'string': ['object', 'string'],
            'bool': ['bool'],
            'datetime': ['datetime64', 'datetime64[ns]']
        }
        
        for expected, actuals in type_mappings.items():
            if expected_type.lower() == expected:
                return any(actual in actual_type for actual in actuals)
        
        return actual_type.lower() == expected_type.lower()
