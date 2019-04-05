# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, get_date
from pyspark.sql import functions as fn
from pyspark.storagelevel import StorageLevel


class StockPrdMonCheckExceptionData(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_prd_mon_check_exception_data = self.conf[
            'stock_prd_mon_check_exception_data_table']
        self.stock_cust_return_by_prd_by_month = self.conf[
            'stock_cust_return_by_prd_temp_table']
        self.stock_return_by_month = self.conf["stock_return_by_month_table"]

    def init_data(self):
        print 'init StockPrdMonCheckExceptionData'

    def daily_compute(self, startdate=None, enddate=None):
        month = enddate[0:7]
        sqlPrd = """
            select busi_date,trade_id,month,
            sum(short_return) short_return,
            sum(long_return) long_return,
            max(exception_label) prd_exception_label
            from {1}.{2}
            where month='{0}' and prd_no!='0.0' and trade_id='11305'
            group by trade_id,month,busi_date
        """.format(month, self.adata, self.stock_cust_return_by_prd_by_month)
        dfPrd = self.sparkSession.sql(sqlPrd)\
            .persist(StorageLevel.DISK_ONLY)
        dfPrd.registerTempTable("temp_return_prd_month_1")
        sqlMon = """
            select trade_id,month,
            sum(stock_short_return) short_return,
            sum(stock_long_return) long_return,
            max(case when stock_total_return>=0 and stock_total_return_rate>=0 then 0
                     when stock_total_return<0 and stock_total_return_rate<0 then 0
                     else 1 end) mon_pos_neg_exception,
            max(exception_label) mon_exception_label
            from {1}.{2}
            where  month='{0}' and trade_id='11305'
            group by trade_id,month
        """.format(month, self.adata, self.stock_return_by_month)
        dfMon = self.sparkSession.sql(sqlMon)\
            .persist(StorageLevel.DISK_ONLY)
        dfMon.registerTempTable("temp_return_prd_1")
        sql = """
            select nvl(a.trade_id,b.trade_id) trade_id,
                   nvl(a.month,b.month) month,
                   nvl(mon_pos_neg_exception,0) mon_pos_neg_exception,
                   nvl(prd_exception_label,0) prd_exception_label,
                   nvl(mon_exception_label,0) mon_exception_label,
                   case when abs(nvl(a.short_return,0)-nvl(b.short_return,0)) <= 0.01
                        then 0 else 1 end sr_equal_exception,
                   case when abs(nvl(a.long_return,0)-nvl(b.long_return,0)) <= 0.01
                        then 0 else 1 end lr_equal_exception
            from temp_return_prd_month_1 a
                 full outer join  temp_return_prd_1 b
                 on a.trade_id=b.trade_id and a.month=b.month
        """
        df = self.sparkSession.sql(sql).persist(StorageLevel.DISK_ONLY)\
            .where("""
                prd_exception_label>0 or mon_pos_neg_exception>0 or
                mon_exception_label>0 or sr_equal_exception>0 or
                lr_equal_exception>0
            """)\
            .withColumn("busi_date", fn.lit(enddate)).repartition(5)
        save_data(self.sparkSession, self.adata, self.stock_prd_mon_check_exception_data,
                  enddate, df)
