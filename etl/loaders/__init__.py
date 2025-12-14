"""
Data loaders for PostgreSQL data warehouse
"""
from etl.loaders.base import BaseLoader
from etl.loaders.warehouse_loader import WarehouseLoader

__all__ = ['BaseLoader', 'WarehouseLoader']
