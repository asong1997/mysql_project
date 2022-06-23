"""
创建数据库类：DataBase用于数据连接，并且以DataFrame的数据格式进行操作数据库
"""
from functools import wraps
import dbutils.pooled_db
import sqlalchemy.engine
import yaml
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table
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
    def execute_sql(self, sql):
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

    def sava(self, df, table_name, add_id=False):
        if add_id:
            table_series = table_name[:-1] + 's'
            sql = "select {}.Nextval from (select 1 from all_objects where rownum <= {})".format(table_series,
                                                                                                 df.shape[0])
            ids = self.read_sql(sql)
            df["id"] = ids.iloc[:, 1].values
        if isinstance(self.db_orm, sqlalchemy.engine.base.Engine):
            meta = MetaData()
            table_para = Table(table_name, meta, autoload=True, autoload_with=self.db_orm, schema=self.database)
            df2dict = df.to_dict(orient='records')
            conn = self.connection()
            table_insert = str(table_para.insert())
            conn.execute(table_insert, df2dict)
        elif isinstance(self.db_orm, dbutils.pooled_db.PooledDB):
            col_name = str(tuple(df.columns)).replace("'", "")
            placeholder = '({})'.format(', '.join(['%s'*df.shape[1]]))
            sql = f"insert into {table_name} {col_name} values {placeholder}"
            self.execute_many_sql(sql=sql, data=df.values)

    def update(self):
        pass

    def re_connect(class_method):
        @wraps(class_method)
        def warpped_function(self, *args, **kwargs):
            try:
                func_result = class_method(self, *args, **kwargs)
                return func_result
            except Exception as e:
                print("重新连接数据库")
                try:
                    self.conn = self.connection()
                    func_result = class_method(self, *args, **kwargs)
                    return func_result
                except Exception as e:
                    print("数据库重新连接失败")

        return warpped_function

    @re_run
    def run(self):
        pass

    def exception(self, error_type):
        pass

    def upload(self):
        pass

    def re_run(class_method):
        @wraps(class_method)
        def wrapped_function(self, kwargs):
            try:
                class_method(self, kwargs)
            except NoDataError as e:
                self.exception(kwargs.get("version_id"), "数据异常")
            except Exception as e:
                self.exception(kwargs.get("version_id"), "模型异常")
            finally:
                self.upload()

        return wrapped_function()



if __name__ == '__main__':
    # orm==dbutils/sqlalchemy/null 分别表示以数据连接池/数据引擎/普通连接方式
    datadb = DataBase(orm="sqlalchemy")
    sql = "select * from sys_config"
    df = datadb.read_sql(sql)
    print(df)
