# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, get_date
from pyspark.sql import functions as fn
from pyspark.storagelevel import StorageLevel


class AssetDrCheckExceptionData(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.asset_dr_check_exception_data = self.conf['asset_dr_check_exception_data_table']
        self.asset_cust_daily_return = self.conf['asset_cust_daily_return_table']

    def init_data(self):
        print 'init stock_ac_check_exception_data'

    def daily_compute(self, startdate=None, enddate=None):
        startdate = get_date(self.date_order, self.order_date, enddate, -19)
        sql = """
            select busi_date,trade_id,return_type_1 return,return_rate_type_1 return_rate,
            exception_label ac_exception_label,
            case when return_type_1>=0 and return_rate_type_1>=0 then 0
                 when return_type_1<0 and return_rate_type_1<0 then 0
                 else 1 end pos_neg_exception,
            case when abs(return_rate_type_1)<0.223 then 0
                 else 1 end rr_outlier_exception
            from {1}.{2}
            where busi_date='{0}'
        """
        sqlCmd = sql.format(enddate, self.adata, self.asset_cust_daily_return)
        df = self.sparkSession.sql(sqlCmd)
        sqllast20 = """
            select trade_id,
            avg(return_rate_type_1)+3*stddev(return_rate_type_1) std_return_rate,
            count(busi_date) num_data
            from {2}.{3}
            where busi_date>='{0}' and busi_date<='{1}'
            group by trade_id
        """.format(startdate, enddate, self.adata, self.asset_cust_daily_return)
        dflast20 = self.sparkSession.sql(sqllast20)
        df = df.join(dflast20, "trade_id", "inner")\
            .select(df["*"],
                    fn.when((dflast20.num_data == 20) &
                            (dflast20.std_return_rate <= df.return_rate), 0)
                    .when(dflast20.std_return_rate == 0, 0)
                    .when(dflast20.num_data < 20, 0).otherwise(1).alias("rr_sp_exception"))\
            .where("""
                    ac_exception_label!=0 or pos_neg_exception!=0  or
                    rr_outlier_exception!=0 or rr_sp_exception!=0
                 """)\
            .persist(StorageLevel.DISK_ONLY).repartition(10)
        save_data(self.sparkSession, self.adata, self.asset_dr_check_exception_data, enddate, df)
