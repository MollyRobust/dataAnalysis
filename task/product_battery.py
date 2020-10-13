import pandas as pd

from task import common_function as cf

filepath = 'parameter.env'

# 数据来源，数据库配置信息
fbg_wms_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_wms_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_wms_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_wms_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
wms_sourceDB = "wms_goodcang_com"
engine_wms_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_wms_username, fbg_wms_password,
                                                                      fbg_wms_hostname, fbg_wms_port, wms_sourceDB))

fbg_bsc_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_bsc_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_bsc_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_bsc_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')
bsc_sourceDB = "productextend"
engine_bsc_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_bsc_username, fbg_bsc_password,
                                                                      fbg_bsc_hostname, fbg_bsc_port, bsc_sourceDB))

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')
# 目标数据库
dst_db = 'fbg_mid_dw'
dst_tb = 't_tmp_product_battery_detail'
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dst_db))

# 查wms
wms_sql = """
            select p.product_barcode,
                   p.customer_code,
                   '纯电' as contain_battery,
                   product_real_weight,
                   product_real_height,
                   product_real_width,
                   product_real_length,
                   sum(pi.pi_sellable + pi.pi_reserved + pi.pi_unsellable+pi.pi_stocking) as quantity
              from product p 
              left join product_inventory pi
                on p.product_barcode = pi.product_barcode and p.customer_code = p.customer_code
             where contain_battery = 2
               and product_real_weight >0
             group by p.product_barcode, p.customer_code
"""
wms_df = pd.read_sql(wms_sql, engine_wms_source)

# 查bsc
bsc_sql = """
            
"""



