"""
创建数据库类：DataBase用于数据连接，并且以DataFrame的数据格式进行操作数据库
"""

import yaml
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from dbutils.pooled_db import PooledDB


class DataBase(object):

    def __init__(self, orm="dbutils"):
        self.db_info = self.parse_config_info()
        self.user = self.db_info.get("user")
        self.password = self.db_info.get("password")
        self.host = self.db_info.get("host")
        self.port = self.db_info.get("port")
        self.database = self.db_info.get("database")
        if orm == "dbutils":
            self.db_orm = self.create_db_pool()
        elif orm == "sqlalchemy":
            self.db_orm = self.create_db_engine()
        else:
            self.db_orm = None

        self.conn = self.connection()

    def parse_config_info(self):
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config

    def connection(self):
        # PooledDB
        if hasattr(self.db_orm, 'connection'):
            conn = self.db_orm.connection()
        # create_engine
        elif hasattr(self.db_orm, 'connect'):
            conn = self.db_orm.connect()
        # 普通连接方式
        else:
            conn = self.create_connection()

        return conn

    def create_connection(self):
        conn = pymysql.connect(**self.db_info)
        return conn

    def create_db_pool(self):
        db_pool = PooledDB(creator=pymysql,
                           mincached=5,
                           maxcached=50,
                           maxconnections=100,
                           maxusage=100,
                           **self.db_info)

        return db_pool

    def create_db_engine(self):
        connection_str = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        db_engine = create_engine(connection_str)

        return db_engine


    def read_sql(self, sql):
        conn = self.connection()
        df = pd.read_sql(sql, con=conn)
        df.columns = df.columns.str.upper()
        conn.close()

        return df

    # 单条数据插入/更新
    def execute_sql(self,sql):
        conn = self.connection()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    # 批量数据插入
    def execute_many_sql(self, sql, data):
        conn = self.connection()
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
        cur.close()
        conn.close()

    def sava(self):
        pass

    def update(self):
        pass

if __name__ == '__main__':
    # orm==dbutils/sqlalchemy/null 分别表示以数据连接池/数据引擎/普通连接方式
    datadb = DataBase(orm="null")
    sql = "select * from sys_config"
    df = datadb.read_sql(sql)
    print(df)



