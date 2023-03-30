import mysql.connector
from mysql.connector import Error
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import json
from types import SimpleNamespace
import pandas as pd
from datetime import datetime
import math
import gspread
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime
import pymysql
pymysql.install_as_MySQLdb()

from . import utils

with open('cfg.yaml') as cfg:
    cfg = json.loads(json.dumps(load(cfg, Loader=Loader)), object_hook=lambda d: SimpleNamespace(**d))


#query function
def query(sql, configs):
    try:
        cf_port = configs.port
    except:
        cf_port = 3306
    print(f'try to connect to {configs.name}...')
    conn = mysql.connector.connect(user=configs.user, password=configs.password, \
                                   host=configs.host, database=configs.database, port=cf_port)
    cursor = conn.cursor()
    print('reading...')
    df = pd.read_sql(sql, conn)
    return df


def opt(sql, configs):
    try:
        cf_port = configs.port
    except:
        cf_port = 3306
    print(f'try to connect to {configs.name}...')
    conn = mysql.connector.connect(user=configs.user, password=configs.password, \
                                   host=configs.host, database=configs.database, port=cf_port)
    cursor = conn.cursor()
    cursor.execute(sql)


class Manage_table:
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
            query(sql = f"select * from {self.table} limit 2", configs=configs)
            print("table exist!")
        except:
            print("table not found!")

    def delete_table(self):
        
        conn = mysql.connector.connect(user=self.user, password=self.password, host=self.host, database=self.database, port=self.port)
        cursor = conn.cursor()
        drop_query = "DROP TABLE {}".format(self.table)
        try:
            cursor.execute(drop_query)
            print(f'{self.table} deleted!')
        except:
            print(f'did not find {self.table}!')
        conn.close()

    
    def upload_data(self, data, add_date=True):
        if add_date:
            run_time = datetime.now()
            run_date = datetime.strftime(run_time, '%Y-%m-%d')
            data['rundate'] = run_date
        data=data.convert_dtypes()
        
        engine = f'mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        my_conn = create_engine(engine)

        batch_size = 10000

        bath_qty = math.ceil(len(data)/batch_size)
        for i in tqdm(range(bath_qty)):
            data_tmp = data[i*batch_size:(i+1)*batch_size].reset_index(drop=True)
            print(f'********************** batch {i+1} / {bath_qty} **********************')
            data_tmp.to_sql(con=my_conn,name=self.table ,if_exists='append',index=False, dtype={"run_date": DateTime()})

    def par_del(self, clause):
        conn = mysql.connector.connect(user=self.user, password=self.password, host=self.host, database=self.database, port=self.port)
        cursor = conn.cursor()
        del_query = f"delete from {self.table} where {clause} "
        print(del_query)
        try:
            cursor.execute(del_query)
            print(f'records of table that {clause} are deleted!')
        except:
            print('del error!')

def gs_read_df(map):
    print('...reading google sheets...')
    gc = gspread.service_account()
    wks = gc.open(map[0]).worksheet(map[1])
    df = pd.DataFrame(wks.get_all_records())
    print('...have read google sheets!...')
    print(df.head())
    return df

def gs_write_df(map, df, loc='A1'):
    print('...writing google sheets...')
    gc = gspread.service_account()
    wks = gc.open(map[0]).worksheet(map[1])
    wks.update(loc,([df.columns.values.tolist()] + df.values.tolist()))
    print('data is written!')