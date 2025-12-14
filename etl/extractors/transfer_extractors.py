"""
File transfer extractors for SFTP, FTP, and Google Sheets
"""
from typing import Dict, Any, Optional
import pandas as pd
import paramiko
from ftplib import FTP
import gspread
from google.oauth2.service_account import Credentials
from etl.extractors.base import BaseExtractor
from etl.logger import get_logger
import io

logger = get_logger(__name__)


class SFTPExtractor(BaseExtractor):
    """Extract data from SFTP servers"""
    
    def __init__(self, host: str, username: str, password: Optional[str] = None,
                 private_key_path: Optional[str] = None, port: int = 22,
                 config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("sftp_server", config_dict)
        self.host = host
        self.username = username
        self.password = password
        self.private_key_path = private_key_path
        self.port = port
    
    def extract(self, remote_path: str, file_type: str = 'csv', **kwargs) -> pd.DataFrame:
        """
        Extract file from SFTP server
        
        Args:
            remote_path: Path to file on SFTP server
            file_type: Type of file ('csv', 'json', 'excel', etc.)
        """
        transport = paramiko.Transport((self.host, self.port))
        
        try:
            if self.private_key_path:
                private_key = paramiko.RSAKey.from_private_key_file(self.private_key_path)
                transport.connect(username=self.username, pkey=private_key)
            else:
                transport.connect(username=self.username, password=self.password)
            
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            # Download file to memory
            with sftp.file(remote_path, 'r') as remote_file:
                file_content = remote_file.read()
            
            # Parse based on file type
            buffer = io.BytesIO(file_content)
            
            if file_type == 'csv':
                df = pd.read_csv(buffer)
            elif file_type == 'json':
                df = pd.read_json(buffer)
            elif file_type == 'excel':
                df = pd.read_excel(buffer)
            elif file_type == 'parquet':
                df = pd.read_parquet(buffer)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            logger.info("sftp_extraction_successful", host=self.host, file=remote_path, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("sftp_extraction_failed", host=self.host, file=remote_path, error=str(e))
            raise
        finally:
            transport.close()


class FTPExtractor(BaseExtractor):
    """Extract data from FTP servers"""
    
    def __init__(self, host: str, username: str = '', password: str = '',
                 config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("ftp_server", config_dict)
        self.host = host
        self.username = username
        self.password = password
    
    def extract(self, remote_path: str, file_type: str = 'csv', **kwargs) -> pd.DataFrame:
        """
        Extract file from FTP server
        
        Args:
            remote_path: Path to file on FTP server
            file_type: Type of file ('csv', 'json', 'excel', etc.)
        """
        ftp = FTP(self.host)
        
        try:
            ftp.login(self.username, self.password)
            
            # Download file to memory
            buffer = io.BytesIO()
            ftp.retrbinary(f'RETR {remote_path}', buffer.write)
            buffer.seek(0)
            
            # Parse based on file type
            if file_type == 'csv':
                df = pd.read_csv(buffer)
            elif file_type == 'json':
                df = pd.read_json(buffer)
            elif file_type == 'excel':
                df = pd.read_excel(buffer)
            elif file_type == 'parquet':
                df = pd.read_parquet(buffer)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            logger.info("ftp_extraction_successful", host=self.host, file=remote_path, rows=len(df))
            return df
            
        except Exception as e:
            logger.error("ftp_extraction_failed", host=self.host, file=remote_path, error=str(e))
            raise
        finally:
            ftp.quit()


class GoogleSheetsExtractor(BaseExtractor):
    """Extract data from Google Sheets"""
    
    def __init__(self, credentials_path: str, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("google_sheets", config_dict)
        
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ]
        
        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        self.client = gspread.authorize(creds)
    
    def extract(self, spreadsheet_id: str, worksheet_name: Optional[str] = None,
                worksheet_index: int = 0, **kwargs) -> pd.DataFrame:
        """
        Extract data from Google Sheet
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            worksheet_name: Name of worksheet (optional)
            worksheet_index: Index of worksheet if name not provided
        """
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            if worksheet_name:
                worksheet = spreadsheet.worksheet(worksheet_name)
            else:
                worksheet = spreadsheet.get_worksheet(worksheet_index)
            
            # Get all values
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            
            logger.info("google_sheets_extraction_successful",
                       spreadsheet=spreadsheet_id,
                       worksheet=worksheet_name or worksheet_index,
                       rows=len(df))
            return df
            
        except Exception as e:
            logger.error("google_sheets_extraction_failed",
                        spreadsheet=spreadsheet_id,
                        error=str(e))
            raise
    
    def extract_range(self, spreadsheet_id: str, range_name: str, **kwargs) -> pd.DataFrame:
        """
        Extract specific range from Google Sheet
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            range_name: Range in A1 notation (e.g., 'Sheet1!A1:D10')
        """
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            values = spreadsheet.values_get(range_name)
            
            data = values.get('values', [])
            if not data:
                return pd.DataFrame()
            
            # First row as headers
            df = pd.DataFrame(data[1:], columns=data[0])
            
            logger.info("google_sheets_range_extraction_successful",
                       spreadsheet=spreadsheet_id,
                       range=range_name,
                       rows=len(df))
            return df
            
        except Exception as e:
            logger.error("google_sheets_range_extraction_failed",
                        spreadsheet=spreadsheet_id,
                        range=range_name,
                        error=str(e))
            raise
