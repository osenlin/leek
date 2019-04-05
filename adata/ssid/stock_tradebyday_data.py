# coding=utf-8
from leek.adata.ssid.ssidjob import LeekSparkJob
from leek.common.util import save_data, get_natural_date
from pyspark.sql import functions as fn
from pyspark.storagelevel import StorageLevel


class StockTradeByDayData(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_tradebyday_data = self.conf["stock_tradebyday_data_table"]
        self.stock_close_tradebyday_by_month = self.conf["stock_close_tradebyday_by_month_table"]
        self.stock_close_tradebyday_long_data = self.conf["stock_close_tradebyday_long_data_table"]
        self.stock_close_tradebyday_short_data = self.conf[
            "stock_close_tradebyday_short_data_table"]
        self.stock_unclose_tradebyday_long_data = self.conf[
            "stock_unclose_tradebyday_long_data_table"]
        self.stock_unclose_tradebyday_short_data = self.conf[
            "stock_unclose_tradebyday_short_data_table"]

    def init_data(self):
        print 'init stock_close_tradebyday_by_month'
        print 'init stock_tradebyday_data'

    def _cal_current_month_data(self, startdate, enddate):
        sql = """
            select
              trade_id,secu_acc_id,open_date,return,return_rate,weighted_term*{5} wt,
              exception_label,{5} inv
            from {2}.{3}
            where busi_date>='{0}' and busi_date<='{1}'
            union all
            select
              trade_id,secu_acc_id,open_date,return,return_rate,weighted_term*{6} wt,
              exception_label,{6} inv
            from {2}.{4}
            where busi_date>='{0}' and busi_date<='{1}'
        """
        # 获取每月一号到计算日的close数据
        closeDf = self.sparkSession.sql(
            sql.format(startdate, enddate, self.fdata, self.stock_close_tradebyday_long_data,
                       self.stock_close_tradebyday_short_data, 'trd_amt', 'close_amt'))
        # 获取计算日unclose数据
        unCloseDf = self.sparkSession.sql(
            sql.format(enddate, enddate, self.fdata, self.stock_unclose_tradebyday_long_data,
                       self.stock_unclose_tradebyday_short_data, 'trd_amt',
                       '(close_amt-unclose_amt)'))
        unCloseDf.persist(StorageLevel.DISK_ONLY).count()
        closeDf.persist(StorageLevel.DISK_ONLY).count()
        return closeDf, unCloseDf

    def daily_compute(self, startdate=None, enddate=None):
        """
        daily_compute
        """
        currentMonthDate = enddate[0:7] + '-01'
        print '>>>>get current month date between [{}] and [{}]'.format(currentMonthDate, enddate)
        currentMonthDataDf, currentDateUnCloseDf = self._cal_current_month_data(currentMonthDate,
                                                                                enddate)
        month = enddate[0:7]
        closeMonthDataDf = self._cal_data(currentMonthDataDf, enddate).withColumn("month",
                                                                                  fn.lit(month))
        dropTablePartitionSql = "alter table {0}.{1} drop if exists  partition(month='{2}')".format(
            self.adata, self.stock_close_tradebyday_by_month, month)
        save_data(self.sparkSession, self.adata, self.stock_close_tradebyday_by_month, enddate,
                  closeMonthDataDf.repartition(20), partitonByName=["month"],
                  dropPartitonSql=dropTablePartitionSql)
        startdate = str(get_natural_date(enddate, -360 + 1))
        print '>>>>get last year date between [{}] and [{}]'.format(startdate, enddate)
        df = self._cal_current_data(startdate, enddate, currentDateUnCloseDf)
        save_data(self.sparkSession, self.adata, self.stock_tradebyday_data, enddate,
                  df.repartition(20))

    def _cal_current_data(self, startdate, enddate, unCloseDf):
        tempTable = "temp_tradebyday_unclose_data"
        unCloseDf.createOrReplaceTempView(tempTable)
        sql = """
            select trade_id,secu_acc_id,open_date,return,return_rate,
              weighted_term*inv wt,inv,exception_label
            from {5}.{6}
            where month>='{0}' and month<='{1}' and open_date>='{2}' and open_date<='{3}'
            union ALL
            select trade_id,secu_acc_id,open_date,return,return_rate,wt,inv,exception_label
            from {4}
            where open_date>='{2}'
        """
        df = self.sparkSession.sql(
            sql.format(startdate[0:7], enddate[0:7], startdate, enddate, tempTable, self.adata,
                       self.stock_close_tradebyday_by_month))
        df.persist(StorageLevel.DISK_ONLY).count()
        df = self._cal_data(df, enddate)
        return df

    def _cal_data(self, df, busi_date):
        df = df.groupBy("trade_id", "secu_acc_id", "open_date").agg(
            fn.sum("return").alias("return"), fn.sum("inv").alias("inv"), fn.sum("wt").alias("wt"),
            fn.max("exception_label").alias("exception_label"))
        df.persist(StorageLevel.DISK_ONLY).count()
        df = df.select("trade_id", "secu_acc_id", "open_date", "return", "exception_label", "inv",
                       (df["return"] / df["inv"]).alias("return_rate"),
                       (df.wt / df.inv).alias("weighted_term"))\
            .withColumn("busi_date", fn.lit(busi_date))
        df.persist(StorageLevel.DISK_ONLY).count()
        return df
