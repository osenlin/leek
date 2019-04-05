# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, full_outer_join
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as fn

LOG_LEVEL = 'info'


class StockOCCheckException(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_daily_check_data = self.conf['check_data_table']
        self.stock_close_o_trade_long_data = self.conf['long_close_trade_table']
        self.stock_unclose_o_trade_long_data = self.conf['long_unclose_trade_table']
        self.stock_close_o_trade_short_data = self.conf['short_close_trade_table']
        self.stock_unclose_o_trade_short_data = self.conf['short_unclose_trade_table']
        self.stock_close_c_trade_long_data = self.conf['long_c_close_trade_table']
        self.stock_unclose_c_trade_long_data = self.conf['long_c_unclose_trade_table']
        self.stock_close_c_trade_short_data = self.conf['short_c_close_trade_table']
        self.stock_unclose_c_trade_short_data = self.conf['short_c_unclose_trade_table']
        self.taget_detail_table = self.conf["stock_oc_check_exception_data_table"]
        self.taget_report_table = self.conf["stock_oc_check_exception_report_table"]

    def init_data(self):
        print 'init StockOCCheckException'

    def _cal_data(self, busi_date, database, close_table, unclose_table, return_name, ex_label_name,
                  unclose_amt_name):
        sqlTmp = """
            select trade_id,secu_acc_id,busi_date,prd_no,sum(return) {4},
              max(exception_label) {5},
              sum(unclose_amt) {6}
            from(
                select trade_id,secu_acc_id,busi_date,exception_label,prd_no,
                return,0 unclose_amt
                from {1}.{2}
                where busi_date='{0}'
                union ALL
                select trade_id,secu_acc_id,busi_date,exception_label,prd_no,
                return,unclose_amt
                from {1}.{3}
                where busi_date='{0}'
            ) a
            group by trade_id,secu_acc_id,busi_date,prd_no
        """
        sql = sqlTmp.format(busi_date, database, close_table, unclose_table, return_name,
                            ex_label_name, unclose_amt_name)
        if LOG_LEVEL == 'debug':
            print sql
        df = self.sparkSession.sql(sql)
        return df

    def daily_compute(self, startdate=None, enddate=None):
        # cal：o_return，o_exception_label，c_return，c_exception_label
        df_o_long = self._cal_data(enddate, self.fdata, self.stock_close_o_trade_long_data,
                                   self.stock_unclose_o_trade_long_data, 'o_return',
                                   'o_exception_label', 'o_unclose_amt')
        df_o_short = self._cal_data(enddate, self.fdata, self.stock_close_o_trade_short_data,
                                    self.stock_unclose_o_trade_short_data, 'o_return',
                                    'o_exception_label', 'o_unclose_amt')
        df_o = df_o_long.union(df_o_short).groupBy(
            ["trade_id", "secu_acc_id", "busi_date", "prd_no"]).agg(
            fn.sum("o_return").alias("o_return"),
            fn.max("o_exception_label").alias("o_exception_label"))
        if LOG_LEVEL == 'debug':
            df_o.show()
        df_c_long = self._cal_data(enddate, self.fdata, self.stock_close_c_trade_long_data,
                                   self.stock_unclose_c_trade_long_data, 'c_return',
                                   'c_exception_label', 'c_unclose_amt')
        df_c_short = self._cal_data(enddate, self.fdata, self.stock_close_c_trade_short_data,
                                    self.stock_unclose_c_trade_short_data, 'c_return',
                                    'c_exception_label', 'c_unclose_amt')
        df_c = df_c_long.union(df_c_short).groupBy(
            ["trade_id", "secu_acc_id", "busi_date", "prd_no"]).agg(
            fn.sum("c_return").alias("c_return"),
            fn.max("c_exception_label").alias("c_exception_label"))
        # cal:long/
        ocLong = full_outer_join(df_o_long, df_c_long,
                                 ["trade_id", "secu_acc_id", "busi_date", "prd_no"])
        ocLong.registerTempTable("temp_oc_long_check_1")
        sqlOcTemp = """
            select trade_id,secu_acc_id,busi_date,prd_no,
                   case when abs(nvl(c_return,0.0)-nvl(o_return,0.0))<=0.01
                        then 0 else 1 end {1},
                   case when abs(nvl(c_unclose_amt,0.0)-nvl(o_unclose_amt,0.0))<=0.01
                         then 0 else 1 end {2}
            from {0}
        """
        sqlOcLong = sqlOcTemp.format("temp_oc_long_check_1", "lr_equal_exception",
                                     "lmv_equal_exception")
        df_oc_long = self.sparkSession.sql(sqlOcLong)
        ocShort = full_outer_join(df_o_short, df_c_short,
                                  ["trade_id", "secu_acc_id", "busi_date", "prd_no"])
        ocShort.registerTempTable("temp_oc_short_check_2")
        sqlOcShort = sqlOcTemp.format("temp_oc_short_check_2", "sr_equal_exception",
                                      "smv_equal_exception")
        df_oc_short = self.sparkSession.sql(sqlOcShort)
        df1 = full_outer_join(df_o, df_c, ["trade_id", "secu_acc_id", "busi_date", "prd_no"])
        df2 = full_outer_join(df_oc_long, df_oc_short,
                              ["trade_id", "secu_acc_id", "busi_date", "prd_no"])
        df3 = full_outer_join(df1, df2, ["trade_id", "secu_acc_id", "busi_date", "prd_no"])
        df3.registerTempTable("temp_stockoccheckexception")
        sql = """
                    select trade_id,
                    secu_acc_id,
                    busi_date,
                    prd_no,
                    nvl(o_return,0) o_return,
                    nvl(o_exception_label,0) o_exception_label,
                    nvl(c_return,0) c_return,
                    nvl(c_exception_label,0) c_exception_label,
                    nvl(lr_equal_exception,0) lr_equal_exception,
                    nvl(lmv_equal_exception,0) lmv_equal_exception,
                    nvl(sr_equal_exception,0) sr_equal_exception,
                    nvl(smv_equal_exception,0) smv_equal_exception
                    from temp_stockoccheckexception
            """
        finalDf = self.sparkSession.sql(sql)
        if LOG_LEVEL == 'debug':
            finalDf.show()
        self._cal_detail(finalDf, enddate)
        self._stat_report(finalDf, enddate)

    def _cal_detail(self, finalDf, enddate):
        finalDfData = finalDf.where("""
                    o_exception_label>0  or c_exception_label>0 or
                    lr_equal_exception>0 or lmv_equal_exception>0 or
                    sr_equal_exception>0 or smv_equal_exception>0
            """)
        save_data(self.sparkSession, self.adata, self.taget_detail_table, enddate, finalDfData)

    def _stat_report(self, df, enddate):
        allCount = df.count()
        finalDf = df.groupBy("busi_date").\
            agg(fn.sum("o_exception_label").alias("o_exception_uv"),
                fn.sum("c_exception_label").alias("c_exception_uv"),
                fn.sum(fn.when(df.o_exception_label != df.c_exception_label, 1).otherwise(0)).alias(
                "o_c_exception_unequal_uv"),
                fn.sum("lr_equal_exception").alias("lr_equal_exception_uv"),
                fn.sum("lmv_equal_exception").alias("lmv_equal_exception_uv"),
                fn.sum("sr_equal_exception").alias("sr_equal_exception_uv"),
                fn.sum("smv_equal_exception").alias("smv_equal_exception_uv"),
                (fn.sum("o_exception_label") / allCount).alias("o_exception_rate"),
                (fn.sum("c_exception_label") / allCount).alias("c_exception_rate"), (fn.sum(
                    fn.when(df.o_exception_label != df.c_exception_label, 1).otherwise(
                        0)) / allCount).alias("o_c_exception_unequal_rate"),
                (fn.sum("lr_equal_exception") / allCount).alias("lr_equal_exception_rate"),
                (fn.sum("lmv_equal_exception") / allCount).alias("lmv_equal_exception_rate"),
                (fn.sum("sr_equal_exception") / allCount).alias("sr_equal_exception_rate"),
                (fn.sum("smv_equal_exception") / allCount).alias("smv_equal_exception_rate"))
        save_data(self.sparkSession, self.adata, self.taget_report_table, enddate, finalDf)
