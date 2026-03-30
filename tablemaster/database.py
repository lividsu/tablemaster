import json
import logging
import re
import warnings
from functools import lru_cache

from sqlalchemy import create_engine, pool, text
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)


def get_connect_args(configs):
    """
    获取数据库连接参数，支持SSL和其他通用配置
    
    Args:
        configs: 配置对象，可以包含以下属性:
            - use_ssl: 是否使用SSL (bool)
            - ssl_ca: SSL证书路径 (str)
            - connect_args: 自定义连接参数 (dict)
            - db_type: 数据库类型 ('tidb', 'mysql' 等)
    
    Returns:
        dict: 连接参数字典
    """
    connect_args = {}
    
    if hasattr(configs, 'connect_args') and configs.connect_args:
        connect_args = configs.connect_args.copy()
    else:
        use_ssl = getattr(configs, 'use_ssl', False)
        db_type = getattr(configs, 'db_type', 'mysql').lower()
        
        if db_type == 'tidb' or use_ssl:
            ssl_ca = getattr(configs, 'ssl_ca', '/etc/ssl/cert.pem')
            connect_args = {
                'ssl': {
                    'ca': ssl_ca,
                    'check_hostname': False,
                    'verify_identity': False
                }
            }
    
    return connect_args


def _build_conn_str(configs):
    db_type = getattr(configs, 'db_type', 'mysql').lower()
    password_encoded = quote_plus(configs.password)
    match db_type:
        case 'mysql' | 'tidb':
            cf_port = getattr(configs, 'port', 3306)
            return f'mysql+pymysql://{configs.user}:{password_encoded}@{configs.host}:{cf_port}/{configs.database}'
        case 'postgresql':
            cf_port = getattr(configs, 'port', 5432)
            return f'postgresql+psycopg2://{configs.user}:{password_encoded}@{configs.host}:{cf_port}/{configs.database}'
        case _:
            raise ValueError(f'Unsupported db_type: {configs.db_type}')


@lru_cache(maxsize=16)
def _get_engine(conn_str, connect_args_json='{}', autocommit=False):
    connect_args = json.loads(connect_args_json) if connect_args_json else {}
    engine_kwargs = {
        'connect_args': connect_args,
        'poolclass': pool.QueuePool,
        'pool_size': 5,
        'max_overflow': 10,
        'pool_pre_ping': True,
    }
    if autocommit:
        engine_kwargs['isolation_level'] = 'AUTOCOMMIT'
    return create_engine(conn_str, **engine_kwargs)


def _resolve_engine(configs, autocommit=False):
    connection_string = _build_conn_str(configs)
    connect_args = get_connect_args(configs)
    connect_args_json = json.dumps(connect_args, sort_keys=True, default=str)
    return _get_engine(connection_string, connect_args_json, autocommit)


def _safe_identifier(identifier):
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', identifier):
        raise ValueError(f'Invalid identifier: {identifier}')
    return identifier


def _safe_mysql_type(data_type):
    normalized = data_type.strip()
    if not re.match(r'^[A-Za-z0-9_,()\s]+$', normalized):
        raise ValueError(f'Invalid data type expression: {data_type}')
    return normalized


def query(sql, configs, params=None):
    logger.info('try to connect to %s...', getattr(configs, 'name', 'database'))
    engine = _resolve_engine(configs, autocommit=False)
    with engine.connect() as conn:
        statement = text(sql) if isinstance(sql, str) else sql
        df = pd.read_sql(statement, conn, params=params)
    logger.debug('query preview: %s', df.head())
    return df


def opt(sql, configs, params=None):
    logger.info('try to connect to %s...', getattr(configs, 'name', 'database'))
    engine = _resolve_engine(configs, autocommit=True)
    with engine.connect() as conn:
        statement = text(sql) if isinstance(sql, str) else sql
        conn.execute(statement, params or {})
    logger.info('database execute success')


class ManageTable:
    def __init__(self, table, configs, verify=False):
        self.port = getattr(configs, 'port', 3306)
        self.table = table
        self.name = configs.name
        self.user = configs.user
        self.password = configs.password
        self.host = configs.host
        self.database = configs.database
        self.configs = configs
        if verify:
            self._check_exists()

    def _check_exists(self):
        if not self.exists():
            raise ValueError(f'table not found: {self.table}')
        logger.info('table exists: %s', self.table)

    def exists(self):
        safe_table = _safe_identifier(self.table)
        check_sql = text(f'SELECT 1 FROM `{safe_table}` LIMIT 1')
        try:
            opt(check_sql, self)
            return True
        except Exception:
            return False

    def delete_table(self):
        safe_table = _safe_identifier(self.table)
        try:
            opt(text(f'DROP TABLE `{safe_table}`'), self)
            logger.info('%s deleted', self.table)
        except Exception:
            logger.exception('table was not deleted')

    def par_del(self, clause, params=None):
        safe_table = _safe_identifier(self.table)
        del_clause = text(f'DELETE FROM `{safe_table}` WHERE {clause}')
        opt(del_clause, self, params=params)
        logger.info('records deleted by clause: %s', clause)

    def change_data_type(self, cols_name, data_type):
        safe_table = _safe_identifier(self.table)
        safe_col = _safe_identifier(cols_name)
        safe_type = _safe_mysql_type(data_type)
        change_clause = text(f'ALTER TABLE `{safe_table}` MODIFY COLUMN `{safe_col}` {safe_type}')
        opt(change_clause, self)
        logger.info('%s changed to %s successfully', cols_name, data_type)


    def upload_data(self, df, chunk_size=10000, add_date=False):
        engine = _resolve_engine(self.configs if hasattr(self, 'configs') else self, autocommit=False)

        with engine.begin() as connection:
            if add_date:
                df_copy = df.copy()
                df_copy['rundate'] = datetime.now().strftime('%Y-%m-%d')
            else:
                df_copy = df
            total_chunks = (len(df_copy) // chunk_size) + (0 if len(df_copy) % chunk_size == 0 else 1)
            logger.info('try to upload data now, chunk_size is %s', chunk_size)
            with tqdm(total=total_chunks, desc="Uploading Chunks", unit="chunk") as pbar:
                try:
                    for start in range(0, len(df_copy), chunk_size):
                        end = min(start + chunk_size, len(df_copy))
                        chunk = df_copy.iloc[start:end]
                        chunk.to_sql(name=self.table, con=connection, if_exists='append', index=False)
                        pbar.update(1)
                except Exception as e:
                    logger.exception('an error occurred during upload: %s', e)

    def upsert_data(self, df, chunk_size=10000, add_date=False, ignore=False, key=None):
        engine = _resolve_engine(self.configs if hasattr(self, 'configs') else self, autocommit=False)
        db_type = getattr(self.configs if hasattr(self, 'configs') else self, 'db_type', 'mysql').lower()

        with engine.begin() as connection:
            if add_date:
                df_copy = df.copy()
                df_copy['rundate'] = datetime.now().strftime('%Y-%m-%d')
            else:
                df_copy = df

            total_chunks = (len(df_copy) // chunk_size) + (0 if len(df_copy) % chunk_size == 0 else 1)
            logger.info('trying to upload data now, chunk_size is %s', chunk_size)

            with tqdm(total=total_chunks, desc="Uploading Chunks", unit="chunk") as pbar:
                for start in range(0, len(df_copy), chunk_size):
                    end = min(start + chunk_size, len(df_copy))
                    chunk = df_copy.iloc[start:end]
                    columns = chunk.columns.tolist()
                    value_placeholders = ', '.join([f':{col}' for col in columns])

                    try:
                        if ignore == False:
                            if db_type in ('mysql', 'tidb'):
                                update_columns = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in columns])
                                insert_sql = f"""
                                INSERT INTO {self.table} ({', '.join([f'`{col}`' for col in columns])})
                                VALUES ({value_placeholders})
                                ON DUPLICATE KEY UPDATE {update_columns}
                                """
                            elif db_type == 'postgresql':
                                if not key:
                                    raise ValueError('key is required for postgresql upsert')
                                safe_key = _safe_identifier(key)
                                safe_columns = [_safe_identifier(col) for col in columns]
                                quoted_columns = ', '.join([f'"{col}"' for col in safe_columns])
                                update_columns = ', '.join(
                                    [f'"{col}"=EXCLUDED."{col}"' for col in safe_columns if col != safe_key]
                                )
                                if update_columns:
                                    insert_sql = f"""
                                    INSERT INTO {self.table} ({quoted_columns})
                                    VALUES ({value_placeholders})
                                    ON CONFLICT ("{safe_key}") DO UPDATE SET {update_columns}
                                    """
                                else:
                                    insert_sql = f"""
                                    INSERT INTO {self.table} ({quoted_columns})
                                    VALUES ({value_placeholders})
                                    ON CONFLICT ("{safe_key}") DO NOTHING
                                    """
                            else:
                                raise ValueError(f'Unsupported db_type for upsert: {db_type}')
                        else:
                            insert_sql = f"""
                            INSERT IGNORE INTO {self.table} ({', '.join([f'`{col}`' for col in columns])})
                            VALUES ({value_placeholders})
                            """

                        data = chunk.where(pd.notna(chunk), None).to_dict(orient='records')
                        connection.execute(text(insert_sql), data)
                        pbar.update(1)
                    except Exception as e:
                        logger.exception('an error occurred during upsert: %s', e)

class Manage_table(ManageTable):
    def __init__(self, table, configs, verify=False):
        warnings.warn(
            'Manage_table is deprecated and will be removed in v2.0.0; use ManageTable instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(table, configs, verify=verify)

    def delete_table(self):
        super().delete_table()

    def upload_data(self, df, chunk_size=10000, add_date=True):
        engine = _resolve_engine(self.configs if hasattr(self, 'configs') else self, autocommit=False)

        with engine.begin() as connection:
            if add_date:
                df_copy = df.copy()
                df_copy['rundate'] = datetime.now().strftime('%Y-%m-%d')
            else:
                df_copy = df
            total_chunks = (len(df_copy) // chunk_size) + (0 if len(df_copy) % chunk_size == 0 else 1)
            logger.info('try to upload data now, chunk_size is %s', chunk_size)
            with tqdm(total=total_chunks, desc="Uploading Chunks", unit="chunk") as pbar:
                try:
                    for start in range(0, len(df_copy), chunk_size):
                        end = min(start + chunk_size, len(df_copy))
                        chunk = df_copy.iloc[start:end]
                        chunk.to_sql(name=self.table, con=connection, if_exists='append', index=False)
                        pbar.update(1)
                except Exception as e:
                    logger.exception('an error occurred during upload: %s', e)
