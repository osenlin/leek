# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, full_outer_join, get_date
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as fn


class StockCDCCheckException(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_close_c_trade_long_data = self.conf['long_c_close_trade_table']
        self.stock_unclose_c_trade_long_data = self.conf['long_c_unclose_trade_table']
        self.stock_close_c_trade_short_data = self.conf['short_c_close_trade_table']
        self.stock_unclose_c_trade_short_data = self.conf['short_c_unclose_trade_table']
        self.check_data_table = self.conf['check_data_table']
        self.taget_detail_table = self.conf["stock_cdc_check_exception_data_table"]
        self.taget_report_table = self.conf["stock_cdc_check_exception_report_table"]

    def init_data(self):
        print 'init StockCDCCheckException'

    def _cal_data(self, busi_date, database, close_table, unclose_table, return_name, ex_label_name,
                  unclose_amt_name, type="all"):
        sqlTmp = """
            select trade_id,secu_acc_id,busi_date,prd_no,sum(return) {3},
              max(exception_label) {4},
              sum(unclose_amt) {5}
            from(
                select trade_id,secu_acc_id,busi_date,exception_label,prd_no,
                return,0 unclose_amt
                from {1}.{2}
                where busi_date='{0}'
            ) a
            group by trade_id,secu_acc_id,busi_date,prd_no

        """
        sql1 = sqlTmp.format(busi_date, self.fdata, close_table, return_name,
                             ex_label_name, unclose_amt_name)
        sqlTmp2 = """
            select trade_id,secu_acc_id,busi_date,prd_no,sum(return) {3},
              max(exception_label) {4},
              sum(unclose_amt) {5}
            from {1}.{2}
            where busi_date='{0}'
            group by trade_id,secu_acc_id,busi_date,prd_no
        """
        sql2 = sqlTmp2.format(busi_date, self.fdata, unclose_table, return_name, ex_label_name,
                              unclose_amt_name)
        if type == 'all':
            sql = sql1
        else:
            sql = sql2
        df = self.sparkSession.sql(sql)
        return df

    def _cal_dc(self, busi_date, trd_type):
        sqlTmp = """
         select trade_id,secu_acc_id,busi_date,prd_no,sum(return) dc_return,
         max(case when qty_exception>return_rate_exception
                  then qty_exception else return_rate_exception end) dc_exception_label,
         sum(now_mkt_val) dc_unclose_amt
         from {1}.{2}
         where busi_date='{0}' and trd_type='{3}' and prd_no!='0.0'
         group by trade_id,secu_acc_id,busi_date,prd_no
        """.format(busi_date, self.fdata, self.check_data_table, trd_type)
        return self.sparkSession.sql(sqlTmp).persist(StorageLevel.DISK_ONLY)

    def _cal_return(self, busi_date,
                    oreturn="c_return", oept="c_exception_label", ounclose="c_unclose_amt",
                    type="all"):
        # 计算T日的return
        df_c_long = self._cal_data(busi_date, self.fdata, self.stock_close_c_trade_long_data,
                                   self.stock_unclose_c_trade_long_data, oreturn,
                                   oept, ounclose, type)
        df_c_short = self._cal_data(busi_date, self.fdata, self.stock_close_c_trade_short_data,
                                    self.stock_unclose_c_trade_short_data, oreturn,
                                    oept, ounclose, type)
        df_c = df_c_long.union(df_c_short).groupBy(
            ["trade_id", "secu_acc_id", "prd_no"]).agg(
            fn.sum(oreturn).alias(oreturn),
            fn.sum(ounclose).alias(ounclose),
            fn.max(oept).alias(oept))
        return df_c, df_c_long, df_c_short

    def daily_compute(self, startdate=None, enddate=None):
        # cal：c_return，c_exception_label，c_return，c_exception_label
        #获取闭仓收益的数据
        df_c, df_c_long, df_c_short = self._cal_return(enddate)
        df_dc_long = self._cal_dc(enddate, 'long_related')
        df_dc_short = self._cal_dc(enddate, 'short_related')
        #获取daily_check_data的数据
        df_dc = df_dc_long.union(df_dc_short).groupBy(
            ["trade_id", "secu_acc_id", "prd_no"]).agg(
            fn.sum("dc_return").alias("dc_return"),
            fn.max("dc_exception_label").alias("dc_exception_label"))
        # cal:long/
        dfall = full_outer_join(df_c, df_dc, ["trade_id", "secu_acc_id", "prd_no"])
        dfall.registerTempTable("temp_all_return")
        sql_all_return = """
                   select trade_id,secu_acc_id,prd_no,
                         nvl(c_return,0) c_return, nvl(dc_return,0) dc_return,
                         nvl(c_exception_label,0) c_exception_label,
                         nvl(dc_exception_label,0) dc_exception_label
                   from {0}
               """.format("temp_all_return")
        dfall = self.sparkSession.sql(sql_all_return)
        dfLong = full_outer_join(df_c_long, df_dc_long, ["trade_id", "secu_acc_id", "prd_no"])
        dfLong.registerTempTable("temp_all_long_return")
        sqlOcTemp = """
            select trade_id,secu_acc_id,prd_no,
                   case when abs(nvl(c_return,0.0)-nvl(dc_return,0.0))<=0.01
                        then 0 else 1 end {1},
                   case when abs(nvl(c_unclose_amt,0.0)-nvl(dc_unclose_amt,0.0))<=0.01
                         then 0 else 1 end {2}
            from {0}
        """
        sqlOcLong = sqlOcTemp.format("temp_all_long_return", "lr_equal_exception",
                                     "lmv_euqal_exception")
        df_cdc_long = self.sparkSession.sql(sqlOcLong)
        dfShort = full_outer_join(df_c_short, df_dc_short, ["trade_id", "secu_acc_id", "prd_no"])
        dfShort.registerTempTable("temp_all_short_return")
        sqlOcShort = sqlOcTemp.format("temp_all_short_return", "sr_equal_exception",
                                      "smv_equal_exception")
        df_cdc_short = self.sparkSession.sql(sqlOcShort)
        df2 = full_outer_join(df_cdc_long, df_cdc_short, ["trade_id", "secu_acc_id", "prd_no"])
        df3 = full_outer_join(dfall, df2, ["trade_id", "secu_acc_id", "prd_no"])
        df3.registerTempTable("temp_stockodccheckexception")
        sql = """select trade_id,
                    secu_acc_id,
                    '{0}' busi_date,
                    prd_no,
                    nvl(c_return,0) c_return,
                    nvl(c_exception_label,0) c_exception_label,
                    nvl(dc_return,0) dc_return,
                    nvl(dc_exception_label,0) dc_exception_label,
                    nvl(lr_equal_exception,0) lr_equal_exception,
                    nvl(lmv_euqal_exception,0) lmv_euqal_exception,
                    nvl(sr_equal_exception,0) sr_equal_exception,
                    nvl(smv_equal_exception,0) smv_equal_exception
                 from temp_stockodccheckexception
        """.format(enddate)
        finalDf = self.sparkSession.sql(sql)
        self._cal_detail(finalDf, enddate)
        self._stat_report(finalDf, enddate)

    def _cal_detail(self, finalDf, enddate):
        finalDfData = finalDf.where("""
                    c_exception_label>0  or dc_exception_label>0 or
                    lr_equal_exception>0 or lmv_euqal_exception>0 or
                    sr_equal_exception>0 or smv_equal_exception>0
        """)
        save_data(self.sparkSession, self.adata, self.taget_detail_table, enddate, finalDfData)

    def _stat_report(self, df, enddate):
        allCount = df.count()
        finalDf = df.groupBy("busi_date").\
            agg(fn.sum("c_exception_label").alias("c_exception_uv"),
                fn.sum("dc_exception_label").alias("dc_exception_uv"),
                fn.sum(fn.when(df.c_exception_label != df.dc_exception_label,
                               1).otherwise(0)).alias("c_dc_exception_unequal_uv"),
                fn.sum("lr_equal_exception").alias("lr_equal_exception_uv"),
                fn.sum("lmv_euqal_exception").alias("lmv_equal_exception_uv"),
                fn.sum("sr_equal_exception").alias("sr_equal_exception_uv"),
                fn.sum("smv_equal_exception").alias("smv_equal_exception_uv"),
                (fn.sum("c_exception_label") / allCount).alias("c_exception_rate"),
                (fn.sum("dc_exception_label") / allCount).alias("dc_exception_rate"),
                (fn.sum(fn.when(df.c_exception_label != df.dc_exception_label, 1).otherwise(
                        0)) / allCount).alias("c_dc_exception_unequal_rate"),
                (fn.sum("lr_equal_exception") / allCount).alias("lr_equal_exception_rate"),
                (fn.sum("lmv_euqal_exception") / allCount).alias("lmv_equal_exception_rate"),
                (fn.sum("sr_equal_exception") / allCount).alias("sr_equal_exception_rate"),
                (fn.sum("smv_equal_exception") / allCount).alias("smv_equal_exception_rate"))
        save_data(self.sparkSession, self.adata, self.taget_report_table, enddate, finalDf)
