"""
File-based extractors for CSV, JSON, Parquet, Excel, and XML
"""
from typing import Dict, Any, Optional
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET
from etl.extractors.base import BaseExtractor
from etl.logger import get_logger

logger = get_logger(__name__)


class CSVExtractor(BaseExtractor):
    """Extract data from CSV files"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("csv_file", config_dict)
    
    def extract(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from CSV file
        
        Args:
            file_path: Path to CSV file
            **kwargs: Additional pandas read_csv arguments
        """
        try:
            df = pd.read_csv(
                file_path,
                encoding=kwargs.get('encoding', 'utf-8'),
                sep=kwargs.get('sep', ','),
                header=kwargs.get('header', 0),
                **{k: v for k, v in kwargs.items() if k not in ['encoding', 'sep', 'header']}
            )
            logger.info("csv_extraction_successful", file=file_path, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("csv_extraction_failed", file=file_path, error=str(e))
            raise


class JSONExtractor(BaseExtractor):
    """Extract data from JSON files"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("json_file", config_dict)
    
    def extract(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from JSON file
        
        Args:
            file_path: Path to JSON file
            **kwargs: Additional pandas read_json arguments
        """
        try:
            df = pd.read_json(
                file_path,
                orient=kwargs.get('orient', 'records'),
                lines=kwargs.get('lines', False),
                **{k: v for k, v in kwargs.items() if k not in ['orient', 'lines']}
            )
            logger.info("json_extraction_successful", file=file_path, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("json_extraction_failed", file=file_path, error=str(e))
            raise


class ParquetExtractor(BaseExtractor):
    """Extract data from Parquet files"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("parquet_file", config_dict)
    
    def extract(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from Parquet file
        
        Args:
            file_path: Path to Parquet file
            **kwargs: Additional pandas read_parquet arguments
        """
        try:
            df = pd.read_parquet(
                file_path,
                engine=kwargs.get('engine', 'pyarrow'),
                **{k: v for k, v in kwargs.items() if k != 'engine'}
            )
            logger.info("parquet_extraction_successful", file=file_path, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("parquet_extraction_failed", file=file_path, error=str(e))
            raise


class ExcelExtractor(BaseExtractor):
    """Extract data from Excel files"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("excel_file", config_dict)
    
    def extract(self, file_path: str, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
        """
        Extract data from Excel file
        
        Args:
            file_path: Path to Excel file
            sheet_name: Name or index of sheet to read
            **kwargs: Additional pandas read_excel arguments
        """
        try:
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                engine=kwargs.get('engine', 'openpyxl'),
                **{k: v for k, v in kwargs.items() if k != 'engine'}
            )
            logger.info("excel_extraction_successful", file=file_path, sheet=sheet_name, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("excel_extraction_failed", file=file_path, error=str(e))
            raise
    
    def extract_all_sheets(self, file_path: str, **kwargs) -> Dict[str, pd.DataFrame]:
        """Extract all sheets from Excel file"""
        try:
            dfs = pd.read_excel(
                file_path,
                sheet_name=None,
                engine=kwargs.get('engine', 'openpyxl'),
                **{k: v for k, v in kwargs.items() if k != 'engine'}
            )
            logger.info("excel_extraction_successful", file=file_path, sheets=len(dfs))
            return dfs
            
        except Exception as e:
            logger.error("excel_extraction_failed", file=file_path, error=str(e))
            raise


class XMLExtractor(BaseExtractor):
    """Extract data from XML files"""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("xml_file", config_dict)
    
    def extract(self, file_path: str, xpath: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Extract data from XML file
        
        Args:
            file_path: Path to XML file
            xpath: XPath expression to select elements
            **kwargs: Additional parsing options
        """
        try:
            # First try pandas built-in XML reader
            try:
                df = pd.read_xml(
                    file_path,
                    xpath=xpath if xpath else './*',
                    **kwargs
                )
                logger.info("xml_extraction_successful", file=file_path, rows=len(df))
                return df
            except:
                # Fallback to manual parsing
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                data = []
                elements = root.findall(xpath) if xpath else list(root)
                
                for elem in elements:
                    row = {}
                    for child in elem:
                        row[child.tag] = child.text
                    if elem.attrib:
                        row.update(elem.attrib)
                    data.append(row)
                
                df = pd.DataFrame(data)
                logger.info("xml_extraction_successful", file=file_path, rows=len(df))
                return df
            
        except Exception as e:
            logger.error("xml_extraction_failed", file=file_path, error=str(e))
            raise
