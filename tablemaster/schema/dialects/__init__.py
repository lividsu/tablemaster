from .base import BaseDialect
from .mysql import MySQLDialect
from .postgresql import PostgreSQLDialect
from .tidb import TiDBDialect


def get_dialect(db_type: str) -> BaseDialect:
    key = (db_type or 'mysql').lower()
    if key == 'mysql':
        return MySQLDialect()
    if key == 'tidb':
        return TiDBDialect()
    if key == 'postgresql':
        return PostgreSQLDialect()
    raise ValueError(f'Unsupported db_type: {db_type}')


__all__ = ['BaseDialect', 'MySQLDialect', 'TiDBDialect', 'PostgreSQLDialect', 'get_dialect']
