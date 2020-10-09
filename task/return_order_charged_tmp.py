import pandas as pd
from task import common_function as cf

filepath = 'parameter.env'

# 起止时间
start_date = '2020-01-01'
end_date = '2020-01-31'

# 数据来源，数据库配置信息
fbg_bsc_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_bsc_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_bsc_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_bsc_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')
bscDb_list = ['gc_bsc_amc_202001', 'gc_bsc_amc_202002', 'gc_bsc_amc_202003', 'gc_bsc_amc_202004', 'gc_bsc_amc_202005', 'gc_bsc_amc_202006',
              'gc_bsc_amc_202007', 'gc_bsc_amc_202008', 'gc_bsc_amc_202009']
engine_bsc = ("mysql+pymysql://%s:%s@%s:%d/?charset=utf8" % (fbg_bsc_username, fbg_bsc_password,
                                                             fbg_bsc_hostname, fbg_bsc_port))

# 数据来源，数据库配置信息
fbg_wms_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_wms_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_wms_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_wms_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
wmsDb = 'wms_goodcang_com'
engine_wms = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_wms_username, fbg_wms_password,
                                                               fbg_wms_hostname, fbg_wms_port, wmsDb))

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 目标数据库
dstDb = 'fbg_mid_dw'
dstTb_class = 't_tmp_return_order_charged_class'
dstTb_total = 't_tmp_return_order_charged_total'

# 构建目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dstDb
conn_dest["charset"] = "utf8"
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dstDb))

# bsc查退件单号
df_list = []
for bscDb in bscDb_list:
    query_bsc = """select OrderNumber
                     from %s.WarehouseOrder
                    where AddTime >= '%s'
                      and AddTime <= '%s'
                      and BusinessType = 'as'
                      and ChargeType in (0,2) """ % (bscDb, start_date, end_date)

    print(query_bsc)
    df = pd.read_sql(query_bsc, engine_bsc)
    df_list.append(df)
df_bsc = pd.concat(df_list)
print(df_bsc)
# 将订单连成字符串
returnNum_str = "','".join(list(df_bsc.OrderNumber))
print(returnNum_str)

# wms查数据
# 1、分类型
query_wms_class = """select asro_code, order_code, asro.customer_code, tracking_no, asro.sm_code,
                      case when asro_status = 0 then '客户新建预报'
                           when asro_status = 1 then '待收货'
                           when asro_status = 2 then '待确认'
                           when asro_status = 3 then '已确认'
                           when asro_status = 4 then '拣货中'
                           when asro_status = 5 then '拣货完成'
                           when asro_status = 6 then '质检中'
                           when asro_status = 7 then '质检完成'
                           when asro_status = 8 then '上架中'
                           when asro_status = 9 then '处理完成'
                           when asro_status = 10 then '废弃'
                           when asro_status = 11 then '差异'
                           when asro_status = 12 then '待暂存'
                           when asro_status = 13 then '收货完成'
                           when asro_status = 14 then '销毁'
                        end as asro_status,
                        asro_add_time, bb.ft_code, bb.currency_code, bb.bi_amount
                   from after_sales_return_orders asro
                   JOIN (SELECT bb_refer_code,
                               ft_code,
                               bi_status,
                               bi_amount,
                               currency_code
                          FROM bil_business bb
                          JOIN bil_business_attach bba 
                            ON bb.bb_id=bba.bb_id
                          JOIN bil_income bi 
                            ON bi.bb_id=bba.bb_id
                         WHERE ds_code='as' 
                           and bb_charge_time >= '%s'
                           and bb_charge_time <= '%s'
                        )bb 
                     ON bb.bb_refer_code =asro.asro_code
                  where asro_add_time >= '%s'
                    and asro_add_time <= '%s'
                    and asro_code in ('%s');""" % (start_date, end_date, start_date, end_date, returnNum_str)
print(query_wms_class)
df_class = pd.read_sql(query_wms_class, engine_wms)

# 2、总费用
query_wms_total = """select asro_code, order_code, asro.customer_code, tracking_no, asro.sm_code,
                      case when asro_status = 0 then '客户新建预报'
                           when asro_status = 1 then '待收货'
                           when asro_status = 2 then '待确认'
                           when asro_status = 3 then '已确认'
                           when asro_status = 4 then '拣货中'
                           when asro_status = 5 then '拣货完成'
                           when asro_status = 6 then '质检中'
                           when asro_status = 7 then '质检完成'
                           when asro_status = 8 then '上架中'
                           when asro_status = 9 then '处理完成'
                           when asro_status = 10 then '废弃'
                           when asro_status = 11 then '差异'
                           when asro_status = 12 then '待暂存'
                           when asro_status = 13 then '收货完成'
                           when asro_status = 14 then '销毁'
                        end as asro_status,
                        asro_add_time, bb.currency_code, 
                        sum(bb.bi_amount) as bi_amount
                   from after_sales_return_orders asro
                   JOIN (SELECT bb_refer_code,
                               ft_code,
                               bi_status,
                               bi_amount,
                               currency_code
                          FROM bil_business bb
                          JOIN bil_business_attach bba 
                            ON bb.bb_id=bba.bb_id
                          JOIN bil_income bi 
                            ON bi.bb_id=bba.bb_id
                         WHERE ds_code='as' 
                           and bb_charge_time >= '%s'
                           and bb_charge_time <= '%s'
                        )bb 
                     ON bb.bb_refer_code =asro.asro_code
                  where asro_add_time >= '%s'
                    and asro_add_time <= '%s'
                    and asro_code in ('%s')
                  group by asro_code;""" % (start_date, end_date, start_date, end_date, returnNum_str)
print(query_wms_total)
df_total = pd.read_sql(query_wms_total, engine_wms)

# 删除原有数据
del_class = "delete from %s.%s" % (dstDb, dstTb_class)
cf.sql_caozuo(del_class, conn_dest)
del_total = "delete from %s.%s" % (dstDb, dstTb_total)
cf.sql_caozuo(del_total, conn_dest)

# 3.结果更新写入数据库
cf.df_to_sql(engine_dest, df_class, dstTb_class)
cf.df_to_sql(engine_dest, df_total, dstTb_total)


