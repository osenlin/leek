# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, full_outer_join,get_date
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as fn

LOG_LEVEL = 'debug'


class StockOprdCheckException(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_daily_check_data = self.conf['check_data_table']
        self.stock_close_o_trade_long_data = self.conf['long_close_trade_table']
        self.stock_unclose_o_trade_long_data = self.conf['long_unclose_trade_table']
        self.stock_close_o_trade_short_data = self.conf['short_close_trade_table']
        self.stock_unclose_o_trade_short_data = self.conf['short_unclose_trade_table']
        self.stock_close_prd_long_data = self.conf["long_close_table"]
        self.stock_unclose_prd_long_data = self.conf["long_unclose_table"]
        self.stock_close_prd_short_data = self.conf["short_close_table"]
        self.stock_unclose_prd_short_data = self.conf["short_unclose_table"]
        self.taget_detail_table = self.conf["stock_oprd_check_exception_data_table"]
        self.taget_report_table = self.conf["stock_oprd_check_exception_report_table"]

    def init_data(self):
        print 'init StockOprdCheckException'

    def _cal_data(self, busi_date, database, close_table, unclose_table, return_name, ex_label_name,
                  unclose_amt_name, unclose_amt='unclose_amt', type ='all'):
        sqlTmp1 = """
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
                return,{7} unclose_amt
                from {1}.{3}
                where busi_date='{0}'
            ) a
            group by trade_id,secu_acc_id,busi_date,prd_no
        """
        sql1 = sqlTmp1.format(busi_date, database, close_table, unclose_table, return_name,
                            ex_label_name, unclose_amt_name, unclose_amt)

        sqlTmp2 = """
            select trade_id,secu_acc_id,busi_date,prd_no,sum(return) {3},
              max(exception_label) {4},
              sum({6}) {5}
            from {1}.{2}
            where busi_date='{0}'
            --where trade_id='39' and secu_acc_id='60000082819' and prd_no='2.002030'
            group by trade_id,secu_acc_id,busi_date,prd_no
        """
        sql2 = sqlTmp2.format(busi_date, self.fdata, unclose_table, return_name,
                             ex_label_name, unclose_amt_name, unclose_amt)

        if type == 'all':
            sql = sql1
        else:
            sql = sql2
        df = self.sparkSession.sql(sql)
        return df

    def _cal_return(self, busi_date,
                    oreturn="o_return", tbdreturn="prd_return",
                    oept="o_exception_label", tbdept="prd_exception_label",
                    ounclose="o_unclose_amt", tbdunclose="prd_unclose_amt",
                    unclose_amt='unclose_amt',type="all"):
        # 计算T日的return
        df_o_long = self._cal_data(busi_date, self.fdata, self.stock_close_o_trade_long_data,
                                   self.stock_unclose_o_trade_long_data, oreturn,
                                   oept, ounclose,unclose_amt, type)
        df_o_short = self._cal_data(busi_date, self.fdata, self.stock_close_o_trade_short_data,
                                    self.stock_unclose_o_trade_short_data, oreturn,
                                    oept, ounclose,unclose_amt, type)
        df_o = df_o_long.union(df_o_short).groupBy(
            ["trade_id", "secu_acc_id", "prd_no"]).agg(
            fn.sum(oreturn).alias(oreturn),
            fn.sum(ounclose).alias(ounclose),
            fn.max(oept).alias(oept))
        df_prd_long = self._cal_data(busi_date, self.fdata, self.stock_close_prd_long_data,
                                     self.stock_unclose_prd_long_data, tbdreturn,
                                     tbdept, tbdunclose, 'remain_val', type)

        df_prd_short = self._cal_data(busi_date, self.fdata, self.stock_close_prd_short_data,
                                      self.stock_unclose_prd_short_data, tbdreturn,
                                      tbdept, tbdunclose, 'remain_val', type)
        df_tbd = df_prd_long.union(df_prd_short).groupBy(
            ["trade_id", "secu_acc_id", "prd_no"]).agg(
            fn.sum(tbdreturn).alias(tbdreturn), fn.sum(tbdunclose).alias(tbdunclose),
            fn.max(tbdept).alias(tbdept))
        return df_o, df_o_long, df_o_short, df_tbd, df_prd_long, df_prd_short


    def daily_compute(self, startdate=None, enddate=None):
        # cal：o_return，o_exception_label，c_return，c_exception_label
        df_o, df_o_long, df_o_short, df_tbd, df_prd_long, df_prd_short = self._cal_return(enddate)
        startdate = get_date(self.date_order, self.order_date, enddate, -1)
        df_o_1, df_o_long_1, df_o_short_1, df_prd_1, df_prd_long_1, df_prd_short_1\
            = self._cal_return(startdate,oreturn="o_return_1", tbdreturn="prd_return_1",
                    oept="o_exception_label_1", tbdept="prd_exception_label_1",
                    ounclose="o_unclose_amt_1", tbdunclose="prd_unclose_amt_1", type='unclose')
        df1 = full_outer_join(df_o, df_o_1, ["trade_id", "secu_acc_id", "prd_no"])
        df1.registerTempTable("temp_o_return_table")
        sqlOTemp = """
                  select trade_id,secu_acc_id,prd_no,
                        nvl(o_return,0.0)-nvl(o_return_1,0.0) {1},
                        nvl(o_unclose_amt,0.0)-nvl(o_unclose_amt_1,0.0) {2},
                        nvl(o_exception_label,o_exception_label_1) o_exception_label
            from {0}
        """
        dfOreturn = self.sparkSession.sql(sqlOTemp.format("temp_o_return_table",
                                                          "o_return","o_unclose_amt"))
        dfOLong = full_outer_join(df_o_long, df_o_long_1,
                                 ["trade_id", "secu_acc_id", "prd_no"])
        dfOLong.registerTempTable("temp_o_long_return_table")

        dfOlongreturn = self.sparkSession.sql(sqlOTemp.format("temp_o_long_return_table",
                                              "o_return","o_unclose_amt"))
        dfOlongreturn.show()
        dfOShort = full_outer_join(df_o_short, df_o_short_1, ["trade_id", "secu_acc_id", "prd_no"])
        dfOShort.registerTempTable("temp_o_short_return_table")
        dfOShortreturn = self.sparkSession.sql(sqlOTemp.format("temp_o_short_return_table",
                                              "o_return", "o_unclose_amt"))

        df2 = full_outer_join(df_tbd, df_prd_1, ["trade_id", "secu_acc_id", "prd_no"])
        df2.registerTempTable("temp_prd_return_table")
        sqlTbdTemp = """
                  select trade_id,secu_acc_id,prd_no,
                        nvl(prd_return,0.0)-nvl(prd_return_1,0.0) {1},
                        nvl(prd_unclose_amt,0.0)-nvl(prd_unclose_amt_1,0.0) {2},
                        nvl(prd_exception_label,prd_exception_label_1) prd_exception_label
            from {0}
        """
        dfTbdreturn = self.sparkSession.sql(sqlTbdTemp.format(
            "temp_prd_return_table", "prd_return", "prd_unclose_amt"))
        dfTbdLong = full_outer_join(df_prd_long, df_prd_long_1,
                                    ["trade_id", "secu_acc_id", "prd_no"])
        dfTbdLong.registerTempTable("temp_prd_long_return_table")

        dfTbdlongreturn = self.sparkSession.sql(sqlTbdTemp.format("temp_prd_long_return_table",
                                               "prd_return", "prd_unclose_amt"))
        dfTbdshort = full_outer_join(df_prd_short, df_prd_short_1,
                                    ["trade_id", "secu_acc_id", "prd_no"])
        dfTbdshort.registerTempTable("temp_prd_short_return_table")
        dfTbdShortreturn = self.sparkSession.sql(sqlTbdTemp.format("temp_prd_short_return_table",
                                                 "prd_return", "prd_unclose_amt"))

        dfall = full_outer_join(dfOreturn, dfTbdreturn, ["trade_id", "secu_acc_id", "prd_no"])
        dfall.registerTempTable("temp_all_return")
        sql_all_return="""
            select trade_id,secu_acc_id,prd_no,
                  nvl(o_return,0) o_return, nvl(prd_return,0) prd_return,
                  nvl(o_exception_label,0) o_exception_label,
                  nvl(prd_exception_label,0) prd_exception_label
            from {0}
        """.format("temp_all_return")
        dfall =self.sparkSession.sql(sql_all_return)
        print 'dfall'
        dfall.show()
        dfLong = full_outer_join(dfOlongreturn, dfTbdlongreturn,
                                 ["trade_id", "secu_acc_id", "prd_no"])
        dfLong.registerTempTable("temp_all_long_return")
        sqlOcTemp = """
            select trade_id,secu_acc_id,prd_no,
                   case when abs(nvl(o_return,0.0)-nvl(prd_return,0.0))<=0.01
                        then 0 else 1 end {1},
                   case when abs(nvl(o_unclose_amt,0.0)-nvl(prd_unclose_amt,0.0))<=0.01
                         then 0 else 1 end {2}
            from {0}
        """
        sqlOcLong = sqlOcTemp.format("temp_all_long_return", "lr_equal_exception",
                                     "lmv_euqal_exception")
        df_oc_long = self.sparkSession.sql(sqlOcLong)

        dfShort = full_outer_join(dfOShortreturn, dfTbdShortreturn,
                                  ["trade_id", "secu_acc_id", "prd_no"])

        dfShort.registerTempTable("temp_all_short_return")
        sqlOcShort = sqlOcTemp.format("temp_all_short_return", "sr_equal_exception",
                                      "smv_equal_exception")
        df_oc_short = self.sparkSession.sql(sqlOcShort)

        df2 = full_outer_join(df_oc_long, df_oc_short,
                              ["trade_id", "secu_acc_id", "prd_no"])
        df3 = full_outer_join(dfall, df2, ["trade_id", "secu_acc_id", "prd_no"])
        df3.registerTempTable("temp_stockoprdcheckexception")
        sql = """select trade_id,
                    secu_acc_id,
                    '{0}' busi_date,
                    prd_no,
                    nvl(o_return,0) o_return,
                    nvl(o_exception_label,0) o_exception_label,
                    nvl(prd_return,0) prd_return,
                    nvl(prd_exception_label,0) prd_exception_label,
                    nvl(lr_equal_exception,0) lr_equal_exception,
                    nvl(lmv_euqal_exception,0) lmv_euqal_exception,
                    nvl(sr_equal_exception,0) sr_equal_exception,
                    nvl(smv_equal_exception,0) smv_equal_exception
                  from temp_stockoprdcheckexception
        """.format(enddate)
        finalDf = self.sparkSession.sql(sql)
        self._cal_detail(finalDf, enddate)
        self._stat_report(finalDf, enddate)

    def _cal_detail(self, finalDf, enddate):
        finalDfData = finalDf.where("""
                    o_exception_label>0  or prd_exception_label>0 or
                    lr_equal_exception>0 or lmv_euqal_exception>0 or
                    sr_equal_exception>0 or smv_equal_exception>0
            """)
        save_data(self.sparkSession, self.adata, self.taget_detail_table, enddate, finalDfData)

    def _stat_report(self, df, enddate):
        allCount = df.count()
        finalDf = df.groupBy("busi_date").agg(
            fn.sum("o_exception_label").alias("o_exception_uv"),
            fn.sum("prd_exception_label").alias("prd_exception_uv"),
            fn.sum(fn.when(df.o_exception_label != df.prd_exception_label, 1).otherwise(0)).alias(
                "o_prd_exception_unequal_uv"),
            fn.sum("lr_equal_exception").alias("lr_equal_exception_uv"),
            fn.sum("lmv_euqal_exception").alias("lmv_equal_exception_uv"),
            fn.sum("sr_equal_exception").alias("sr_equal_exception_uv"),
            fn.sum("smv_equal_exception").alias("smv_equal_exception_uv"),
            (fn.sum("o_exception_label") / allCount).alias("o_exception_rate"),
            (fn.sum("prd_exception_label") / allCount).alias("prd_exception_rate"), (fn.sum(
                fn.when(df.o_exception_label != df.prd_exception_label, 1).otherwise(
                    0)) / allCount).alias("o_prd_exception_unequal_rate"),
            (fn.sum("lr_equal_exception") / allCount).alias("lr_equal_exception_rate"),
            (fn.sum("lmv_euqal_exception") / allCount).alias("lmv_equal_exception_rate"),
            (fn.sum("sr_equal_exception") / allCount).alias("sr_equal_exception_rate"),
            (fn.sum("smv_equal_exception") / allCount).alias("smv_equal_exception_rate"))
        save_data(self.sparkSession, self.adata, self.taget_report_table, enddate, finalDf)
