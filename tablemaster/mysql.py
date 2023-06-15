
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import math
import urllib.parse

#query function
def query(sql, configs):
    try:
        cf_port = configs.port
    except:
        cf_port = 3306
    print(f'try to connect to {configs.name}...')
    conn = mysql.connector.connect(user=configs.user, password=configs.password, \
                                   host=configs.host, database=configs.database, port=cf_port)
    print('reading...')
    df = pd.read_sql(sql, conn)
    conn.commit()
    conn.close()
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
    conn.commit()
    cursor.close()
    conn.close()


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
        encoded_pw = urllib.parse.quote(self.password)
        engine = f'mysql://{self.user}:{encoded_pw}@{self.host}:{self.port}/{self.database}'
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
        conn.commit()
        cursor.close()
        conn.close()