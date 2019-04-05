# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, get_date
from pyspark.sql import functions as fn
from pyspark.storagelevel import StorageLevel


class StockDrCheckExceptionData(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_dr_check_exception_data = self.conf['stock_dr_check_exception_data_table']
        self.stock_daily_check_data = self.conf['check_data_table']
        self.stock_cust_daily_return = self.conf['stock_cust_daily_return_table']

    def init_data(self):
        print 'init stock_ac_check_exception_data'

    def daily_compute(self, startdate=None, enddate=None):
        startdate = get_date(self.date_order, self.order_date, enddate, -19)
        sql = """
            select busi_date,trade_id,long_return,short_return,total_return,
            nvl(total_return_rate,0.0) total_return_rate,
            exception_label ac_exception_label,
            case when total_return>=0 and total_return_rate>=0 then 0
                 when total_return<0 and total_return_rate<0 then 0
                 else 1 end pos_neg_exception,
            case when abs(long_return+short_return-total_return)<0.01 then 0
                 else 1 end detail_sum_exception,
            case when abs(total_return_rate)<0.223 then 0
                 else 1 end rr_outlier_exception
            from {1}.{2}
            where busi_date='{0}'
        """
        sqlCmd = sql.format(enddate, self.adata, self.stock_cust_daily_return)
        df = self.sparkSession.sql(sqlCmd)
        sqllast20 = """
            select trade_id,
            avg(total_return_rate)+3*stddev(total_return_rate) std_return_rate,
            count(busi_date) num_data
            from {2}.{3}
            where busi_date>='{0}' and busi_date<='{1}'
            group by trade_id
        """.format(startdate, enddate, self.adata, self.stock_cust_daily_return)
        dflast20 = self.sparkSession.sql(sqllast20)
        df = df.join(dflast20, "trade_id", "inner")\
            .select(df["*"],
                    fn.when((dflast20.num_data == 20) &
                            (dflast20.std_return_rate <= df.total_return_rate), 0)
                    .when(dflast20.num_data < 20, 0)
                    .when(dflast20.std_return_rate == 0, 0)
                    .otherwise(1).alias("rr_sp_exception"))\
            .where("""
                    ac_exception_label!=0 or pos_neg_exception!=0 or
                    detail_sum_exception!=0 or rr_outlier_exception!=0 or
                    rr_sp_exception!=0
                 """)\
            .persist(StorageLevel.DISK_ONLY).repartition(5)
        save_data(self.sparkSession, self.adata, self.stock_dr_check_exception_data, enddate, df)
