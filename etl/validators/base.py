"""
Base validator class
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import pandas as pd
from etl.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Results from a validation check"""
    validator_name: str
    validation_type: str
    passed: bool
    passed_records: int
    failed_records: int
    total_records: int
    error_details: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_records == 0:
            return 0.0
        return (self.passed_records / self.total_records) * 100


class BaseValidator(ABC):
    """Abstract base class for data validators"""
    
    def __init__(self, name: str, strict_mode: bool = False):
        """
        Initialize validator
        
        Args:
            name: Name of the validator
            strict_mode: If True, fail on any validation errors
        """
        self.name = name
        self.strict_mode = strict_mode
    
    @abstractmethod
    def validate(self, df: pd.DataFrame, **kwargs) -> ValidationResult:
        """
        Validate DataFrame
        
        Args:
            df: DataFrame to validate
            **kwargs: Additional validation parameters
            
        Returns:
            ValidationResult object
        """
        pass
    
    def log_result(self, result: ValidationResult) -> None:
        """Log validation results"""
        if result.passed:
            logger.info(
                "validation_passed",
                validator=result.validator_name,
                type=result.validation_type,
                success_rate=f"{result.success_rate:.2f}%",
                passed=result.passed_records,
                failed=result.failed_records
            )
        else:
            logger.warning(
                "validation_failed",
                validator=result.validator_name,
                type=result.validation_type,
                success_rate=f"{result.success_rate:.2f}%",
                passed=result.passed_records,
                failed=result.failed_records,
                errors=len(result.error_details)
            )
    
    def raise_if_failed(self, result: ValidationResult) -> None:
        """Raise exception if validation failed in strict mode"""
        if self.strict_mode and not result.passed:
            raise ValueError(
                f"Validation failed: {result.validator_name} - {result.validation_type}. "
                f"Failed records: {result.failed_records}/{result.total_records}"
            )
