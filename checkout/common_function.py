#!/usr/bin/python3
#-*- coding:utf-8 -*-

import pandas as pd
import pymysql
import sys
from sqlalchemy import create_engine

#读配置文件函数，查找所需的配置
def MySearch(filename,keyword):
    #打开文件
    with open(filename,"r") as myfile:
        list_all=myfile.readlines()

    #逐行查找
    for i in range(0,len(list_all)):
        #查找本行是否有所需的配置，find函数返回查找字符串的开始索引，没找到就返回-1
        if list_all[i].find(keyword)>-1:
            #print(list_all[i])
            return list_all[i].strip('\n').split('=')[1]
    print("Error:未找到"+keyword)  
    sys.exit()

#sql查询数据库，并返回字典形式的结果
def sql_to_df(engine_info, sql):
    '''
    数据库读取为dataframe格式
    :param engine_info:数据库连接引擎
    :param sql:操作的sql语句
    '''
    try:
        # 创建数据库连接引擎
        engine = create_engine(engine_info)
        # 从数据库读取结果为DataFrame
        df_query = pd.read_sql(sql, engine)
        # 关闭数据库连接
        engine.dispose()
        print('数据库读取完毕', df_query.head())
        # 转为字典list
        data = df_query.to_dict(orient='list')
        return data

    except Exception as e:
        print('从数据库读取为df失败:%s' % e)
        return None

#将DataFrame格式数据写入数据表中
def df_to_sql(engine_conn, data, table_name):
    '''
    datafrme格式的数据直接写入数据库
    :param engine_conn:数据库连接引擎
    :param data:要存入的字典格式格式的数据
    :param table_name:要存的表名
    '''
    try:
        #list形式的字典转为dataframe格式
        write_df = pd.DataFrame(data)
        # 创建连接数据库的引擎
        engine = create_engine('%s' % engine_conn)
        # 写入数据
        write_df.to_sql(name=table_name, con=engine, if_exists='append', index=False, index_label=False)
        # 关闭engine的连接
        engine.dispose()
        print('写入数据库成功')

    except Exception as e:
        print('dataframe存入数据库失败：%s' % e)
        return None

#数据库操作，删除，修改，插入等，无返回值
def sql_caozuo(sql, conn):
    '''
    使用sql语句从数据库中一次性读取
    :param sql:操作的说sql语句
    :param conn:数据库连接的字典
    '''
    try:
        # 连接数据库,**为字典传参
        conn = pymysql.connect(**conn)
        cur = conn.cursor()
        # 提交sql语句
        cur.execute(sql)
        # 提交事务执行
        conn.commit()
        # 关闭连接
        cur.close()
        conn.close()

    except Exception as e:
        print('数据库操作失败：%s' % e)
        return None
