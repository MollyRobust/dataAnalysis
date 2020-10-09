import datetime
import time

thrDay = datetime.date.today() + datetime.timedelta(days=-90)
print(thrDay)

# td = datetime.date.today()
# td_str = td.strftime('%Y%m')
# print(td_str)
# month_before = str(datetime.date.today()-datetime.timedelta(days=30))
# print(month_before)
# today = str(datetime.date.today()).split(' ')[0]
# print(datetime.datetime.today())
#
# # 时间戳转格式化字符串
# time_stamp = time.time()
# time_rule = time.localtime(time_stamp)
# time_str = time.strftime('%Y-%m-%d %H:%M:%S',time_rule)
# print(time_rule)
# print(time_stamp)
# print(time_str)
# # print(time.strftime("%Y-%m-%d %HH:%MM:%ss", time.localtime(time_stamp)))
#
# # 时间戳--》datetime
# dt = datetime.datetime.fromtimestamp(time_stamp)
# print(dt,type(dt))
#
# # datetime --> 字符串
# dt_str = datetime.datetime.strftime(dt,'%Y-%m-%d %H:%M:%S')
# dt_str2 = dt.strftime('%Y-%m-%d %H:%M:%S')
# print(dt_str,type(dt_str))
# print(dt_str2,type(dt_str2))
#
# # 时间戳--》字符串
# dt_str3 = datetime.datetime.fromtimestamp(time_stamp).strftime('%Y-%m-%d %H:%M:%S')
# print(dt_str3,type(dt_str3))
#
# # datetime-->时间戳
# dt_stmap = dt.timestamp()
# print(dt_stmap,type(dt_stmap))
#
# # datetime -->结构化对象
# dt_struc = dt.timetuple()
# print(dt_struc,type(dt_struc))
#
# # 结构化对象--》 datetime
# dt2 = datetime.datetime.strptime("2020-09-15 12:46:53","%Y-%m-%d %H:%M:%S")
# print(dt2,type(dt2))
#
# # date + time = datetime
# day = datetime.date.fromtimestamp(time_stamp)
# time = datetime.time(12,31,33,888)
# dtime = datetime.datetime.combine(day,time)
# print(dtime)
#
# query_addTime = ("SELECT orderNumber,addTime FROM %s.%s "
#                  "WHERE orderNumber in ('G666-200801-0080','G1032-200801-0007','G1032-200801-0007','G666-200801-0081');"
#                  )