
from impala.dbapi import connect
from impala.util import as_pandas
import logging
import pandas as pd

class hiveUtil(object):
    def __init__(self,loger=None,retry=1,user='root',password='root'):
        self.retry = retry                 # 重试次数
        self.conn = None                   # 连接对象
        self.hiveHosts = "192.168.79.111"  # 当前连接的机器名
        self.hive_port = 10000
        self.db = "fbg_tmp"                # 当前连接的数据库
        self.user = user
        self.password = password
        self.tables = ["fbg_tmp_bil_wms_busi",
                         "fbg_tmp_bil_wms_busi_new",
                         "fbg_tmp_billflow_order_m",
                         "fbg_tmp_bsc_warehousbillflow_m",
                         "fbg_tmp_wms_customer_balance_log"]
    def __del__(self):
        if self.conn is not None:
            self.conn.close()
            self.conn=None

    def getConn(self):
        """
        获取hive数据库连接
        :return:
        """
        if self.conn is not None:
            self.conn.close()
        conn = None
        try:
            conn =connect(host=self.hiveHosts, port=self.hive_port, database=self.db, auth_mechanism='PLAIN',)
            self.conn = conn
        except Exception as ex:
            msg = '获取hive连接%s失败' % (self.hiveHosts)
            logging.error(msg)
        finally:
            return conn
    def testConn(self):
        conn = self.getConn()
        cur = conn.cursor()
        cur.execute('show tables')
        return cur.fetchall()

    def getQuery(self,table):
        query_sql = (
                '''
                select cbl_refer_code
                  from fbg_busi.fbg_busi_wms_customer_balance_log
                 where cbl_add_time >= '2019-09-01'
                   and cbl_add_time < '2019-09-02'
                   limit 5
                ''')
        conn = self.getConn()
        res_df_pd = pd.read_sql(query_sql, conn)
        # res_df2 = pd.DataFrame(res_dict)
        print(res_df_pd)
        print("--------------------------------")
        res_dict = res_df_pd.to_dict(orient='list')
        print(res_dict)
        return res_df_pd


if __name__ == "__main__":
    p = hiveUtil()
    # res = p.testConn()
    # print(res)
    result = p.getQuery("fbg_tmp_bil_wms_busi")
    # print(result)
