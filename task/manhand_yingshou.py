import datetime
import pandas as pd
from task import common_function as cf

filepath = 'parameter.env'

# 数据来源，数据库配置信息
fbg_bsc_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_bsc_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_bsc_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_bsc_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')
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
dstTb = 't_tmp_manhand_yingshou'

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

# 手工应收数据详情
wms_sql = """
                SELECT bi.bi_amount, 
                       bi.bi_verify_time, 
                       bi.bi_add_time, 
                       bi.customer_code, 
                       bi.bi_note, 
                       bi.create_currency_code, 
                       bba.bb_refer_code, 
                       bba.bb_reference_no, 
                       ft.ft_name_cn, 
                       u1.user_name as set_user, 
                       u2.user_name as check_user,
                       w.warehouse_desc,
                       bfr.account_code
                  FROM bil_income as bi
                  JOIN bil_business as bb
                     ON bb.bb_id=bi.bb_id
                  JOIN bil_business_attach as bba 
                     ON bba.bb_id=bi.bb_id
                LEFT JOIN bsc_fee_relation as bfr 
                     ON bba.bb_refer_code = bfr.bfr_code
                LEFT JOIN fee_type as ft
                     ON ft.ft_code=bi.ft_code
                LEFT JOIN user AS u1 
                     ON u1.user_id=bi.bi_creator_id
                LEFT JOIN warehouse as w ON w.warehouse_id = bb.warehouse_id
                LEFT JOIN user AS u2 
                    ON u2.user_id=bi.bi_verifier_id 
                WHERE bb.bb_status = 'y'
                  AND bb.ds_code in('ot', 'va')
                  AND bi_status >='2' 
                  AND bi_create_type = 1
                  AND bi_verify_time >= '2020-09-01 00:00:00' 
                  AND bi_verify_time <= '2020-09-31 23:59:59'
                ORDER BY bi.bi_add_time desc;
"""
detail_df = pd.read_sql(wms_sql, engine_wms)



bsc_sql = """
           select OrderNumber as bb_refer_code,
                  ChargeType as charge_type
             from gc_bsc_amc_202009.Warehousbillflow 
             group by OrderNumber, ChargeType
"""
type_df = pd.read_sql(bsc_sql, engine_bsc)

# 关联之后得到结果集
res_df = pd.merge(detail_df, type_df, on='bb_refer_code', how='left')
print(res_df)

# 将结果集写进结果表
# 删除原有数据
del_sql = "delete from %s.%s" % (dstDb, dstTb)
cf.sql_caozuo(del_sql, conn_dest)

# 3.结果更新写入数据库
cf.df_to_sql(engine_dest, res_df, dstTb)
