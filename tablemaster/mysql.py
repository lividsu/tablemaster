
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
from tqdm import tqdm


#query function
def query(sql, configs):
    try:
        cf_port = configs.port
    except:
        cf_port = 3306
    print(f'try to connect to {configs.name}...')
    engine = create_engine(f'mysql+pymysql://{configs.user}:{configs.password}@{configs.host}:{cf_port}/{configs.database}')
    df = pd.read_sql(sql, engine)
    print(df.head())
    return df

#opt function
def opt(sql, configs):
    try:
        cf_port = configs.port
    except:
        cf_port = 3306
    print(f'try to connect to {configs.name}...')
    engine = create_engine(f'mysql+pymysql://{configs.user}:{configs.password}@{configs.host}:{cf_port}/{configs.database}',
                           isolation_level="AUTOCOMMIT")
    # Connect to the database using the engine's connect method
    with engine.connect() as conn:
        # Execute the SQL statement directly, without using pandas
        conn.execute(text(sql))
    print('mysql execute success!')

class ManageTable:
    def __init__(self, table, configs):
        try:
            self.port = configs.port
        except:
            self.port = 3306
        self.table = table
        self.name = configs.name
        self.user = configs.user
        self.password = configs.password
        self.host = configs.host
        self.database = configs.database
        try:
            query(sql = f"SELECT * FROM {self.table} LIMIT 1", configs=configs)
            print("table exist!")
        except:
            print("table not found!")

    def delete_table(self):
        try:
            opt(f'DROP TABLE {self.table}', self)
            print(f'{self.table} deleted!')
        except:
            print('Table was not deleted!')

    def par_del(self, clause):
        del_clause = f"DELETE FROM {self.table} WHERE {clause}"
        opt(del_clause, self)
        print(f'records of table that {clause} are deleted!')

    def change_data_type(self, cols_name, data_type):
        change_clause = f'ALTER TABLE {self.table} MODIFY COLUMN {cols_name} {data_type}'
        opt(change_clause, self)
        print(f'{cols_name} changed to {data_type} successfully!')


    def upload_data(self, df, chunk_size=10000, add_date=False):
        db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        with create_engine(db_url).begin() as connection:
            # Add a 'rundate' column with the current date formatted as 'YYYY-MM-DD' if required
            if add_date:
                df_copy = df.copy()
                df_copy['rundate'] = datetime.now().strftime('%Y-%m-%d')
            else:
                df_copy = df
            total_chunks = (len(df_copy) // chunk_size) + (0 if len(df_copy) % chunk_size == 0 else 1)
            print(f'try to upload data now, chunk_size is {chunk_size}')
            with tqdm(total=total_chunks, desc="Uploading Chunks", unit="chunk") as pbar:
                try:
                    for start in range(0, len(df_copy), chunk_size):
                        end = min(start + chunk_size, len(df_copy))
                        chunk = df_copy.iloc[start:end]
                        chunk.to_sql(name=self.table, con=connection, if_exists='append', index=False)
                        pbar.update(1)
                except Exception as e:
                    print(f"An error occurred: {e}")

# Old ManageTable Way
class Manage_table:
    def __init__(self, table, configs):
        print('We recommend using ManageTable for MySQL table management instead of Manage_table. e.g. tb=ManageTable(...)')
        try:
            self.port = configs.port
        except:
            self.port = 3306
        self.table = table
        self.name = configs.name
        self.user = configs.user
        self.password = configs.password
        self.host = configs.host
        self.database = configs.database
        try:
            query(sql = f"SELECT * FROM {self.table} LIMIT 1", configs=configs)
            print("table exist!")
        except:
            print("table not found!")

    def delete_table(self):
        opt(f'DROP TABLE {self.table}', self)
        print(f'{self.table} deleted!')

    def par_del(self, clause):
        del_clause = f"DELETE FROM {self.table} WHERE {clause}"
        opt(del_clause, self)
        print(f'records of table that {clause} are deleted!')

    def change_data_type(self, cols_name, data_type):
        change_clause = f'ALTER TABLE {self.table} MODIFY COLUMN {cols_name} {data_type}'
        opt(change_clause, self)
        print(f'{cols_name} changed to {data_type} successfully!')


    def upload_data(self, df, chunk_size=10000, add_date=True):
        db_url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        with create_engine(db_url).begin() as connection:
            # Add a 'rundate' column with the current date formatted as 'YYYY-MM-DD' if required
            if add_date:
                df_copy = df.copy()
                df_copy['rundate'] = datetime.now().strftime('%Y-%m-%d')
            else:
                df_copy = df
            total_chunks = (len(df_copy) // chunk_size) + (0 if len(df_copy) % chunk_size == 0 else 1)
            print(f'try to upload data now, chunk_size is {chunk_size}')
            with tqdm(total=total_chunks, desc="Uploading Chunks", unit="chunk") as pbar:
                try:
                    for start in range(0, len(df_copy), chunk_size):
                        end = min(start + chunk_size, len(df_copy))
                        chunk = df_copy.iloc[start:end]
                        chunk.to_sql(name=self.table, con=connection, if_exists='append', index=False)
                        pbar.update(1)
                except Exception as e:
                    print(f"An error occurred: {e}")