#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd
import pymysql
import sys
import datetime
from sqlalchemy import create_engine
import json
import urllib.request

from task import common_function as cf

filepath = 'parameter.env'

# 数据来源，数据库配置信息
fbg_source_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_source_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_source_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_source_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')

# 数据中间仓库，数据库配置信息

fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 目标数据库
dbdest = 'fbg_mid_dw'
dstable = 't_bsc_feeflow_repeat_monitor_2'

# bsc按月份分库 gc_bsc_amc_202004
now_time = datetime.datetime.now()
year = now_time.strftime('%Y')
month = now_time.strftime('%m')
sourceDB = 'gc_bsc_amc_' + year + month

today = datetime.date.today()
dtdate = str(today)

# 构建目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dbdest
conn_dest["charset"] = "utf8"

engine_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_source_username, fbg_source_password,
                                                                  fbg_source_hostname, fbg_source_port, sourceDB))
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dbdest))

# # 1、删除目标表中当前仓库数据，这个表存临时数据
# delete_sql = ("delete from %s.%s where dtdate='%s' " % (dbdest, dstable, dtdate))
# # 调用函数来操作数据库，删除数据
# cf.sql_caozuo(delete_sql, conn_dest)

# 2、查询源数据库数据
query_sql = (
            """select '%s' as dtdate, CustomerCode, OrderNumber,
                      TypesOfFee, FlowType, CurrencyCode, Amount,
                      AddTime, count(1) repeat_cnt 
                 from %s.warehousbillflow 
                where addtime>=date_sub(current_date(),interval 6 day) 
                group by CustomerCode,OrderNumber,TypesOfFee,FlowType,CurrencyCode,Amount,AddTime 
               having repeat_cnt>1 """ % (dtdate, sourceDB))
print(query_sql)
# 结果保存在字典query_df中
query_df = cf.sql_to_df(engine_source, query_sql)
print(query_df)
# 3、将查询出来的数据存入目标表
# cf.df_to_sql(engine_dest, query_df, dstable)
#
# ##############################################
# # 发钉钉通知代码
# ##############################################
# # 你的钉钉机器人url
# global my_url
# my_url = "https://oapi.dingtalk.com/robot/send?access_token=658f03ca4da4ab047d53347f2bd9bcedec1b2bb3fa3f400768f8a0fe646c5bac"
#
#
# def send_request(url, datas):
#     # 传入url和内容发送请求
#     # 构建一下请求头部
#     header = {
#         "Content-Type": "application/json",
#         "Charset": "UTF-8"
#     }
#     sendData = json.dumps(datas, ensure_ascii=False)  # 将字典类型数据转化为json格式
#     print(sendData)
#     sendDatas = sendData.encode("utf-8")  # python3的Request要求data为byte类型
#     # 发送请求
#     request = urllib.request.Request(url=url, data=sendDatas, headers=header)
#     # 将请求发回的数据构建成为文件格式
#     opener = urllib.request.urlopen(request)
#     # 打印返回的结果
#     print(opener.read())
#
#
# def get_ddmodel_datas(type):
#     # 返回钉钉模型数据，1:文本；2:markdown所有人；3:markdown带图片，@接收人；4:link类型
#     if type == 1:
#         my_data = {
#             "msgtype": "text",
#             "text": {
#                 "content": " "
#             },
#             "at": {
#                 "atMobiles": [
#                     "188XXXXXXX"
#                 ],
#                 "isAtAll": False
#             }
#         }
#     elif type == 2:
#         my_data = {
#             "msgtype": "markdown",
#             "markdown": {"title": " ",
#                          "text": " "
#                          },
#             "at": {
#                 "isAtAll": True
#             }
#         }
#     elif type == 3:
#         my_data = {
#             "msgtype": "markdown",
#             "markdown": {"title": " ",
#                          "text": " "
#                          },
#             "at": {
#                 "atMobiles": [
#                     "188XXXXXXXX"
#                 ],
#                 "isAtAll": False
#             }
#         }
#     elif type == 4:
#         my_data = {
#             "msgtype": "link",
#             "link": {
#                 "text": " ",
#                 "title": " ",
#                 "picUrl": "",
#                 "messageUrl": " "
#             }
#         }
#     return my_data
#
#
# # 获取sql数据
# query_sql = ("SELECT count(1) icnt"
#              " FROM t_bsc_feeflow_repeat_monitor"
#              " WHERE dtdate>=date_sub(current_date(),interval 1 day)")
#
# # 结果保存在字典query_df中
# query_df = cf.sql_to_df(engine_dest, query_sql)
#
# icnt = query_df["icnt"][0]
# print(icnt)
# if icnt:
#     send_msg = "BSC系统有%s单重复计费" % icnt
#
#     # 4.Link类型群发消息
#     my_data = get_ddmodel_datas(4)
#     my_data["link"]["text"] = "BSC流水重复详情"
#     my_data["link"]["title"] = send_msg
#     my_data["link"][
#         "messageUrl"] = "http://datareport.eminxing.com/d/jXsiRGRGk/bscxi-tong-zhong-fu-fei-yong-liu-shui-jian-kong?orgId=1"
#     send_request(my_url, my_data)
# else:
#     print("BSC没有重复流水")
