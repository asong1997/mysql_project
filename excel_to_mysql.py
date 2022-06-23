"""
将Excel数据读取存入Mysql数据库
方法一：df.to_sql()
方法二：普通方式
"""

import pandas as pd
from sqlalchemy import create_engine
from Mysql_DB import DataBase

df = pd.read_excel("空气质量模拟数据.xlsx")
df.index.name = "id"

# 连接数据库
# datadb = DataBase(orm="sqlalchemy")


"""方法一"""
# 1.当数据表不存在的时候，会新建表再存数据
# df.to_sql(name="air_quality_data", con=datadb.db_orm, if_exists="replace")
# 查看建表语句
# print(datadb.db_orm.execute("show create table air_quality_data").first()[1])
# 查看数据前五行
# print(datadb.db_orm.execute("select * from air_quality_data limit 5").fetchall())

# 2.当数据表存在的时候，追加数据
# df.to_sql(name="air_quality_data", con=datadb.db_orm, if_exists="append")


"""方法二：表需要提前建好"""
datadb = DataBase(orm="null")
conn = datadb.connection()
df = pd.read_csv("MA.csv")
for idx, row in df.iterrows():
    print(f"process:{row.ds}")
    sql = f""" 
            insert into ma 
            (ds, y, pdcp_sduoctul, DtchPrbAssnMeanUl_Rate, DtchPrbAssnMeandl_Rate)
            values ('{row.ds}', '{row.y}', '{row.pdcp_sduoctul}', '{row.DtchPrbAssnMeanUl_Rate}', '{row.DtchPrbAssnMeandl_Rate}')
           """
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()

conn.close()
print("end")

