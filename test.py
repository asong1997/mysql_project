import pymysql


def mysql_db():
    # 连接数据库肯定需要一些参数
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        database="sys",
        charset="utf8",
        user="root",
        passwd="123456"
    )
    return conn


if __name__ == '__main__':
    import pandas as pd
    conn = mysql_db()
    sql = "select * from sys_config"
    df = pd.read_sql(sql, con=conn)
    print(df)





