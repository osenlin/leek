# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data
from pyspark.storagelevel import StorageLevel


class StockPrdMonCheckExceptionReport(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_prd_mon_check_exception_data = self.conf[
            'stock_prd_mon_check_exception_data_table']
        self.stock_prd_mon_check_exception_report = self.conf[
            'stock_prd_mon_check_exception_report_table']
        self.stock_cust_return_by_prd_by_month = self.conf[
            'stock_cust_return_by_prd_temp_table']
        self.stock_return_by_month = self.conf["stock_return_by_month_table"]

    def init_data(self):
        print 'init stock_ac_check_exception_report'

    def _get_all_num(self, enddate, database, table):
        month = enddate[0:7]
        sql = """
            select count(distinct trade_id) all_user
            from {1}.{2}
            where month='{3}'
        """.format(enddate, database, table, month)
        return self.sparkSession.sql(sql)

    def daily_compute(self, startdate=None, enddate=None):
        dfPrd = self._get_all_num(enddate, self.adata, self.stock_cust_return_by_prd_by_month)
        dfMon = self._get_all_num(enddate, self.adata, self.stock_return_by_month)
        allMonNum = dfMon.first().all_user
        allPrdNum = dfPrd.first().all_user
        month = enddate[0:7]
        sqlTmp = """
            select busi_date,month,
                sum(mon_exception_label) mon_exception_uv,
                cast(sum(mon_exception_label)*1.0/{4} as double) mon_exception_rate,
                sum(prd_exception_label) prd_exception_uv,
                cast(sum(prd_exception_label)*1.0/{5} as double) prd_exception_rate,
                sum(lr_equal_exception) lr_exception_uv,
                cast(sum(lr_equal_exception)*1.0/{5} as double) lr_exception_rate,
                sum(sr_equal_exception) sr_exception_uv,
                cast(sum(sr_equal_exception)*1.0/{5} as double) sr_exception_rate,
                sum(mon_pos_neg_exception) mon_pos_neg_exception_uv,
                cast(sum(mon_pos_neg_exception)*1.0/{4} as double) mon_pos_neg_exception_rate
            from  {1}.{2}
            where busi_date= '{0}' and month='{3}'
            group by busi_date,month
        """.format(enddate, self.adata, self.stock_prd_mon_check_exception_data, month, allMonNum,
                   allPrdNum)
        df = self.sparkSession.sql(sqlTmp).persist(StorageLevel.DISK_ONLY).repartition(5)
        save_data(self.sparkSession, self.adata, self.stock_prd_mon_check_exception_report, enddate,
                  df)
