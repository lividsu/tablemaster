import json
import logging
import re
import ssl
import warnings
from typing import Union, List, Tuple, Dict, Any, Optional
from functools import lru_cache

from sqlalchemy import create_engine, inspect, pool, text
from sqlalchemy.engine import Engine, URL
import pandas as pd
from datetime import datetime
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_connect_args(configs: Any) -> Dict[str, Any]:
    """
    Get database connection arguments, supporting SSL and other common configurations.
    
    Args:
        configs (Any): Configuration object that may contain use_ssl, ssl_ca, connect_args, db_type.
    
    Returns:
        Dict[str, Any]: A dictionary of connection arguments.
    """
    connect_args: Dict[str, Any] = {}
    
    if hasattr(configs, 'connect_args') and configs.connect_args:
        connect_args = configs.connect_args.copy()
    else:
        use_ssl: bool = getattr(configs, 'use_ssl', False)
        db_type: str = getattr(configs, 'db_type', 'mysql').lower()
        
        if db_type == 'postgresql' and use_ssl:
            verify_cert = bool(getattr(configs, 'ssl_verify_cert', True))
            verify_identity = bool(getattr(configs, 'ssl_verify_identity', True))
            if verify_identity:
                ssl_mode = 'verify-full'
            elif verify_cert:
                ssl_mode = 'verify-ca'
            else:
                ssl_mode = 'require'
            connect_args = {'sslmode': ssl_mode}
            if getattr(configs, 'ssl_ca', None):
                connect_args['sslrootcert'] = configs.ssl_ca
        elif db_type == 'tidb' or use_ssl:
            ssl_ca: Optional[str] = getattr(configs, 'ssl_ca', None)
            if not ssl_ca:
                ssl_ca = ssl.get_default_verify_paths().cafile
            ssl_options: Dict[str, Any] = {
                'check_hostname': bool(getattr(configs, 'ssl_verify_identity', True)),
                'verify_mode': bool(getattr(configs, 'ssl_verify_cert', True)),
            }
            if ssl_ca:
                ssl_options['ca'] = ssl_ca
            connect_args = {
                'ssl': ssl_options
            }
    
    return connect_args


def _build_conn_str(configs: Any) -> str:
    """
    Build the SQLAlchemy connection string based on configuration.
    
    Args:
        configs (Any): Configuration object containing host, port, user, password, database, etc.
        
    Returns:
        str: The SQLAlchemy connection string.
    """
    db_type: str = getattr(configs, 'db_type', 'mysql').lower()
    match db_type:
        case 'mysql' | 'tidb':
            cf_port: int = getattr(configs, 'port', 3306)
            drivername = 'mysql+pymysql'
        case 'postgresql':
            cf_port: int = getattr(configs, 'port', 5432)
            drivername = 'postgresql+psycopg2'
        case _:
            raise ValueError(f'Unsupported db_type: {configs.db_type}')
    return URL.create(
        drivername=drivername,
        username=configs.user,
        password=configs.password,
        host=configs.host,
        port=cf_port,
        database=configs.database,
    ).render_as_string(hide_password=False)


@lru_cache(maxsize=16)
def _get_engine(conn_str: str, connect_args_json: str = '{}', autocommit: bool = False) -> Engine:
    """
    Get or create a cached SQLAlchemy Engine instance.
    
    Args:
        conn_str (str): The database connection string.
        connect_args_json (str, optional): JSON string representation of connection arguments. Defaults to '{}'.
        autocommit (bool, optional): Whether the engine should be in autocommit mode. Defaults to False.
        
    Returns:
        Engine: The created SQLAlchemy Engine instance.
    """
    connect_args: Dict[str, Any] = json.loads(connect_args_json) if connect_args_json else {}
    engine_kwargs: Dict[str, Any] = {
        'connect_args': connect_args,
        'poolclass': pool.QueuePool,
        'pool_size': 5,
        'max_overflow': 10,
        'pool_pre_ping': True,
    }
    if autocommit:
        engine_kwargs['isolation_level'] = 'AUTOCOMMIT'
    return create_engine(conn_str, **engine_kwargs)


def _resolve_engine(configs: Any, autocommit: bool = False) -> Engine:
    """
    Resolve and return an Engine based on configuration.
    
    Args:
        configs (Any): Configuration object.
        autocommit (bool, optional): Whether to use autocommit mode. Defaults to False.
        
    Returns:
        Engine: The SQLAlchemy Engine instance.
    """
    connection_string: str = _build_conn_str(configs)
    connect_args: Dict[str, Any] = get_connect_args(configs)
    connect_args_json: str = json.dumps(connect_args, sort_keys=True, default=str)
    return _get_engine(connection_string, connect_args_json, autocommit)


def _safe_identifier(identifier: str) -> str:
    """
    Ensure an identifier is safe from SQL injection.
    
    Args:
        identifier (str): The SQL identifier to validate.
        
    Returns:
        str: The safe identifier.
        
    Raises:
        ValueError: If the identifier contains invalid characters.
    """
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', identifier):
        raise ValueError(f'Invalid identifier: {identifier}')
    return identifier


def _safe_qualified_identifier(identifier: str) -> List[str]:
    parts = [part.strip() for part in identifier.split('.')]
    if not parts or any(not part for part in parts):
        raise ValueError(f'Invalid identifier: {identifier}')
    return [_safe_identifier(part) for part in parts]


def _quote_identifier(identifier: str, db_type: str) -> str:
    safe = _safe_identifier(identifier)
    quote = '"' if db_type == 'postgresql' else '`'
    return f'{quote}{safe}{quote}'


def _quote_table(table: str, db_type: str) -> str:
    return '.'.join(_quote_identifier(part, db_type) for part in _safe_qualified_identifier(table))


def _safe_postgresql_table(table: str) -> str:
    return '.'.join(_safe_qualified_identifier(table))


def _execute_postgresql_values(connection: Any, sql: str, rows: List[Tuple[Any, ...]], page_size: int) -> None:
    from psycopg2.extras import execute_values

    proxied_connection = connection.connection
    dbapi_connection = getattr(proxied_connection, 'driver_connection', proxied_connection)
    with dbapi_connection.cursor() as cursor:
        execute_values(cursor, sql, rows, page_size=page_size)


def _safe_mysql_type(data_type: str) -> str:
    """
    Ensure a MySQL data type expression is safe from SQL injection.
    
    Args:
        data_type (str): The MySQL data type to validate.
        
    Returns:
        str: The safe data type string.
        
    Raises:
        ValueError: If the data type expression contains invalid characters.
    """
    normalized: str = data_type.strip()
    if not re.match(r'^[A-Za-z0-9_,()\s]+$', normalized):
        raise ValueError(f'Invalid data type expression: {data_type}')
    return normalized


def _validated_dataframe_columns(df: pd.DataFrame) -> List[str]:
    columns = df.columns.tolist()
    if not all(isinstance(column, str) for column in columns):
        raise ValueError('DataFrame column names must all be strings')
    return [_safe_identifier(column) for column in columns]


def _normalize_key_columns(
    key: Union[str, List[str], Tuple[str, ...], None],
    columns: List[str],
) -> List[str]:
    if key is None:
        return []
    if isinstance(key, str):
        keys = [item.strip() for item in key.split(',') if item.strip()]
    elif isinstance(key, (list, tuple)):
        keys = [str(item).strip() for item in key if str(item).strip()]
    else:
        raise ValueError('key must be a string or a list of strings')
    keys = [_safe_identifier(item) for item in keys]
    missing = [item for item in keys if item not in columns]
    if missing:
        raise ValueError(f'key columns not found in DataFrame: {", ".join(missing)}')
    return keys


def query(sql: Union[str, text], configs: Any, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Execute a query and return results as a pandas DataFrame.
    
    Args:
        sql (Union[str, text]): The SQL query to execute.
        configs (Any): Configuration object.
        params (Optional[Dict[str, Any]], optional): Query parameters. Defaults to None.
        
    Returns:
        pd.DataFrame: Query results.
    """
    logger.info('try to connect to %s...', getattr(configs, 'name', 'database'))
    engine: Engine = _resolve_engine(configs, autocommit=False)
    with engine.connect() as conn:
        statement = text(sql) if isinstance(sql, str) else sql
        df: pd.DataFrame = pd.read_sql(statement, conn, params=params)
    logger.debug('query preview: %s', df.head())
    return df


def opt(sql: Union[str, text], configs: Any, params: Optional[Dict[str, Any]] = None) -> None:
    """
    Execute a SQL statement that modifies the database (e.g., INSERT, UPDATE, DELETE).
    
    Args:
        sql (Union[str, text]): The SQL statement to execute.
        configs (Any): Configuration object.
        params (Optional[Dict[str, Any]], optional): Query parameters. Defaults to None.
    """
    logger.info('try to connect to %s...', getattr(configs, 'name', 'database'))
    engine: Engine = _resolve_engine(configs, autocommit=True)
    with engine.connect() as conn:
        statement = text(sql) if isinstance(sql, str) else sql
        conn.execute(statement, params or {})
    logger.info('database execute success')


class ManageTable:
    """
    A class to manage a specific database table's operations.
    """
    def __init__(self, table: str, configs: Any, verify: bool = False) -> None:
        """
        Initialize a ManageTable instance.
        
        Args:
            table (str): The name of the table.
            configs (Any): Configuration object for the database.
            verify (bool, optional): Whether to verify if the table exists upon initialization. Defaults to False.
        """
        _safe_qualified_identifier(table)
        self.port: int = getattr(configs, 'port', 3306)
        self.table: str = table
        self.name: str = configs.name
        self.user: str = configs.user
        self.password: str = configs.password
        self.host: str = configs.host
        self.database: str = configs.database
        self.configs: Any = configs
        if verify:
            self._check_exists()

    def _check_exists(self) -> None:
        """
        Check if the table exists and raise an error if not.
        
        Raises:
            ValueError: If the table does not exist.
        """
        if not self.exists():
            raise ValueError(f'table not found: {self.table}')
        logger.info('table exists: %s', self.table)

    def exists(self) -> bool:
        """
        Check if the table exists in the database.
        
        Returns:
            bool: True if table exists, False otherwise.
        """
        table_parts = _safe_qualified_identifier(self.table)
        safe_table = table_parts[-1]
        schema = table_parts[-2] if len(table_parts) == 2 else None
        if len(table_parts) > 2:
            raise ValueError(f'Invalid table identifier: {self.table}')
        try:
            engine: Engine = _resolve_engine(self.configs if hasattr(self, 'configs') else self, autocommit=False)
            inspector = inspect(engine)
            return inspector.has_table(safe_table, schema=schema)
        except Exception as e:
            logger.exception('failed to check if table exists: %s', e)
            raise

    def delete_table(self) -> None:
        """
        Drop the table from the database.
        """
        db_type = getattr(self.configs, 'db_type', 'mysql').lower()
        safe_table = _quote_table(self.table, db_type)
        try:
            opt(text(f'DROP TABLE {safe_table}'), self.configs)
            logger.info('%s deleted', self.table)
        except Exception as e:
            logger.exception('table was not deleted: %s', e)
            raise

    def par_del(self, clause: str, params: Optional[Dict[str, Any]] = None) -> None:
        """
        Delete specific records from the table based on a WHERE clause.
        
        Args:
            clause (str): The WHERE clause conditions.
            params (Optional[Dict[str, Any]], optional): Parameters for the WHERE clause. Defaults to None.
        """
        if not clause or not clause.strip():
            raise ValueError('DELETE clause must not be empty')
        db_type = getattr(self.configs, 'db_type', 'mysql').lower()
        safe_table = _quote_table(self.table, db_type)
        del_clause = text(f'DELETE FROM {safe_table} WHERE {clause}')
        opt(del_clause, self.configs, params=params)
        logger.info('records deleted by clause: %s', clause)

    def change_data_type(self, cols_name: str, data_type: str) -> None:
        """
        Change the data type of a specific column in the table.
        
        Args:
            cols_name (str): The name of the column to alter.
            data_type (str): The new data type expression.
        """
        db_type = getattr(self.configs, 'db_type', 'mysql').lower()
        safe_table = _quote_table(self.table, db_type)
        safe_col = _quote_identifier(cols_name, db_type)
        safe_type: str = _safe_mysql_type(data_type)
        if db_type == 'postgresql':
            sql = f'ALTER TABLE {safe_table} ALTER COLUMN {safe_col} TYPE {safe_type}'
        elif db_type in ('mysql', 'tidb'):
            sql = f'ALTER TABLE {safe_table} MODIFY COLUMN {safe_col} {safe_type}'
        else:
            raise ValueError(f'Unsupported db_type: {db_type}')
        opt(text(sql), self.configs)
        logger.info('%s changed to %s successfully', cols_name, data_type)


    def upload_data(self, df: pd.DataFrame, chunk_size: int = 10000, add_date: bool = False) -> None:
        """
        Upload data from a pandas DataFrame to the database table.
        
        Args:
            df (pd.DataFrame): The DataFrame containing data to upload.
            chunk_size (int, optional): Number of rows to upload per chunk. Defaults to 10000.
            add_date (bool, optional): Whether to append the current date to the DataFrame before uploading. Defaults to False.
        """
        if chunk_size <= 0:
            raise ValueError('chunk_size must be positive')
        _validated_dataframe_columns(df)
        engine: Engine = _resolve_engine(self.configs if hasattr(self, 'configs') else self, autocommit=False)
        table_parts = _safe_qualified_identifier(self.table)
        if len(table_parts) > 2:
            raise ValueError(f'Invalid table identifier: {self.table}')
        schema = table_parts[-2] if len(table_parts) == 2 else None
        table_name = table_parts[-1]

        with engine.begin() as connection:
            if add_date:
                df_copy: pd.DataFrame = df.copy()
                df_copy['rundate'] = datetime.now().strftime('%Y-%m-%d')
            else:
                df_copy: pd.DataFrame = df
            total_chunks: int = (len(df_copy) // chunk_size) + (0 if len(df_copy) % chunk_size == 0 else 1)
            logger.info('try to upload data now, chunk_size is %s', chunk_size)
            with tqdm(total=total_chunks, desc="Uploading Chunks", unit="chunk") as pbar:
                try:
                    for start in range(0, len(df_copy), chunk_size):
                        end: int = min(start + chunk_size, len(df_copy))
                        chunk: pd.DataFrame = df_copy.iloc[start:end]
                        chunk.to_sql(
                            name=table_name,
                            schema=schema,
                            con=connection,
                            if_exists='append',
                            index=False,
                        )
                        pbar.update(1)
                except Exception as e:
                    logger.exception('an error occurred during upload: %s', e)
                    raise

    def upsert_data(self, df: pd.DataFrame, chunk_size: int = 10000, add_date: bool = False, ignore: bool = False, key: Union[str, List[str], Tuple[str, ...], None] = None) -> None:
        """
        Upsert data from a pandas DataFrame into the database table.
        
        This method will perform an "insert or update" (upsert) operation based on the target database type.
        If the record already exists (based on the specified primary key or unique index), it updates the existing record.
        Otherwise, it inserts a new record.

        Args:
            df (pd.DataFrame): The pandas DataFrame containing the data to be upserted.
            chunk_size (int, optional): The number of rows to insert per batch. Defaults to 10000.
            add_date (bool, optional): Whether to add a 'rundate' column with the current date to the dataframe. Defaults to False.
            ignore (bool, optional): If True, it performs an 'INSERT IGNORE' or 'ON CONFLICT DO NOTHING' operation, skipping existing records instead of updating them. Defaults to False.
            key (Union[str, List[str], Tuple[str, ...], None], optional): The primary key or unique index column(s) used to detect conflicts. 
                                                                           Required for PostgreSQL. For MySQL/TiDB, this is used to exclude primary key columns from being updated.
                                                                           Can be a comma-separated string or a list/tuple of strings. Defaults to None.
        
        Raises:
            ValueError: If 'key' is not provided when 'db_type' is 'postgresql', or if an unsupported 'db_type' is used.
        """
        if chunk_size <= 0:
            raise ValueError('chunk_size must be positive')
        input_columns = _validated_dataframe_columns(df)
        effective_columns = [*input_columns, *([] if not add_date else ['rundate'])]
        keys = _normalize_key_columns(key, effective_columns)
        db_type: str = getattr(self.configs if hasattr(self, 'configs') else self, 'db_type', 'mysql').lower()
        if db_type not in {'mysql', 'tidb', 'postgresql'}:
            raise ValueError(f'Unsupported db_type for upsert: {db_type}')
        if db_type == 'postgresql' and not ignore and not keys:
            raise ValueError('key is required for postgresql upsert')
        engine = _resolve_engine(self.configs if hasattr(self, 'configs') else self, autocommit=False)

        with engine.begin() as connection:
            if add_date:
                df_copy: pd.DataFrame = df.copy()
                df_copy['rundate'] = datetime.now().strftime('%Y-%m-%d')
            else:
                df_copy: pd.DataFrame = df

            total_chunks: int = (len(df_copy) // chunk_size) + (0 if len(df_copy) % chunk_size == 0 else 1)
            logger.info('trying to upload data now, chunk_size is %s', chunk_size)

            with tqdm(total=total_chunks, desc="Uploading Chunks", unit="chunk") as pbar:
                for start in range(0, len(df_copy), chunk_size):
                    end: int = min(start + chunk_size, len(df_copy))
                    chunk: pd.DataFrame = df_copy.iloc[start:end]
                    columns = _validated_dataframe_columns(chunk)
                    value_placeholders: str = ', '.join([f':{col}' for col in columns])

                    try:
                        if ignore == False:
                            if db_type in ('mysql', 'tidb'):
                                safe_table = _quote_table(self.table, db_type)
                                if keys:
                                    safe_keys = [_safe_identifier(k) for k in keys]
                                    update_columns = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in columns if col not in safe_keys])
                                else:
                                    update_columns = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in columns])
                                    
                                if update_columns:
                                    insert_sql = f"""
                                    INSERT INTO {safe_table} ({', '.join([f'`{col}`' for col in columns])})
                                    VALUES ({value_placeholders})
                                    ON DUPLICATE KEY UPDATE {update_columns}
                                    """
                                else:
                                    insert_sql = f"""
                                    INSERT IGNORE INTO {safe_table} ({', '.join([f'`{col}`' for col in columns])})
                                    VALUES ({value_placeholders})
                                    """
                            elif db_type == 'postgresql':
                                safe_keys = [_safe_identifier(k) for k in keys]
                                safe_columns = [_safe_identifier(col) for col in columns]
                                safe_table = _safe_postgresql_table(self.table)
                                quoted_columns = ', '.join([f'"{col}"' for col in safe_columns])
                                update_columns = ', '.join(
                                    [f'"{col}"=EXCLUDED."{col}"' for col in safe_columns if col not in safe_keys]
                                )
                                conflict_keys_str = ', '.join([f'"{k}"' for k in safe_keys])
                                
                                if update_columns:
                                    insert_sql = f"""
                                    INSERT INTO {safe_table} ({quoted_columns})
                                    VALUES %s
                                    ON CONFLICT ({conflict_keys_str}) DO UPDATE SET {update_columns}
                                    """
                                else:
                                    insert_sql = f"""
                                    INSERT INTO {safe_table} ({quoted_columns})
                                    VALUES %s
                                    ON CONFLICT ({conflict_keys_str}) DO NOTHING
                                    """
                                data_frame = chunk.astype(object).where(pd.notna(chunk), None)
                                data = [tuple(row) for row in data_frame.itertuples(index=False, name=None)]
                                _execute_postgresql_values(connection, insert_sql, data, page_size=chunk_size)
                                pbar.update(1)
                                continue
                            else:
                                raise ValueError(f'Unsupported db_type for upsert: {db_type}')
                        else:
                            if db_type in ('mysql', 'tidb'):
                                safe_table = _quote_table(self.table, db_type)
                                insert_sql = f"""
                                INSERT IGNORE INTO {safe_table} ({', '.join([f'`{col}`' for col in columns])})
                                VALUES ({value_placeholders})
                                """
                            elif db_type == 'postgresql':
                                safe_columns = [_safe_identifier(col) for col in columns]
                                safe_table = _safe_postgresql_table(self.table)
                                quoted_columns = ', '.join([f'"{col}"' for col in safe_columns])
                                insert_sql = f"""
                                INSERT INTO {safe_table} ({quoted_columns})
                                VALUES %s
                                ON CONFLICT DO NOTHING
                                """
                                data_frame = chunk.astype(object).where(pd.notna(chunk), None)
                                data = [tuple(row) for row in data_frame.itertuples(index=False, name=None)]
                                _execute_postgresql_values(connection, insert_sql, data, page_size=chunk_size)
                                pbar.update(1)
                                continue
                            else:
                                raise ValueError(f'Unsupported db_type for upsert: {db_type}')

                        data = chunk.astype(object).where(pd.notna(chunk), None).to_dict(orient='records')
                        connection.execute(text(insert_sql), data)
                        pbar.update(1)
                    except Exception as e:
                        logger.exception('an error occurred during upsert: %s', e)
                        raise

class Manage_table(ManageTable):
    """
    Deprecated class for managing database tables. Use ManageTable instead.
    """
    def __init__(self, table: str, configs: Any, verify: bool = False) -> None:
        """
        Initialize the Manage_table instance. Issues a deprecation warning.
        
        Args:
            table (str): The name of the table to manage.
            configs (Any): Configuration object containing database connection details.
            verify (bool, optional): Whether to verify the table configuration. Defaults to False.
        """
        warnings.warn(
            'Manage_table is deprecated and will be removed in v3.0.0; use ManageTable instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(table, configs, verify=verify)

    def delete_table(self) -> None:
        """
        Drop the table from the database.
        """
        super().delete_table()

    def upload_data(self, df: pd.DataFrame, chunk_size: int = 10000, add_date: bool = True) -> None:
        """
        Upload data from a pandas DataFrame to the database table.
        
        Args:
            df (pd.DataFrame): The pandas DataFrame containing the data to upload.
            chunk_size (int, optional): The number of rows to insert per batch. Defaults to 10000.
            add_date (bool, optional): Whether to add a 'rundate' column with the current date to the dataframe. Defaults to True.
        """
        super().upload_data(df, chunk_size=chunk_size, add_date=add_date)
