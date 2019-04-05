# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, full_outer_join
from pyspark.sql import functions as fn


class StockInvPlCheckException(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_investment_ability = self.conf['stock_cust_investment_ability_table']
        self.stock_cust_pl_analysis = self.conf['stock_cust_pl_analysis_table']
        self.taget_detail_table = self.conf["stock_inv_pl_check_exception_data_table"]
        self.taget_report_table = self.conf["stock_inv_pl_check_exception_report_table"]

    def init_data(self):
        print 'Init StockInvPlCheckException'

    def _cal_rr_sum_exception(self, busi_date):
        sqlTmp = """
            select trade_id,busi_date,compute_term,
                   max(case when abs(return_rate-re_from_selection-re_from_market)<=0.0001
                        then 0 else 1 end) rr_sum_exception
            from {1}.{2}
            where busi_date='{0}'
            group by trade_id,busi_date,compute_term
        """
        sql = sqlTmp.format(busi_date, self.adata, self.stock_cust_investment_ability)
        df = self.sparkSession.sql(sql)
        return df

    def _cal_r_sum_exception(self, busi_date):
        sqlTmp = """select trade_id,busi_date,compute_term,
                           max(case when abs(total_return-total_gain-total_loss)<=0.01
                                then 0 else 1 end) r_sum_exception,
                           max(case when abs(trade_gain_num+trade_loss_num-trade_num)=0
                                then 0 else 1 end) trd_sum_exception
                    from {1}.{2}
                    where busi_date='{0}'
                    group by trade_id,busi_date,compute_term
                """
        sql = sqlTmp.format(busi_date, self.adata, self.stock_cust_pl_analysis)
        df = self.sparkSession.sql(sql)
        return df

    def daily_compute(self, startdate=None, enddate=None):
        df1 = self._cal_rr_sum_exception(enddate)
        df2 = self._cal_r_sum_exception(enddate)
        df3 = full_outer_join(df1, df2, ["trade_id", "busi_date", "compute_term"])
        df3.registerTempTable("temp_stockinvplcheckexception")
        sql = """select trade_id,
                    busi_date,
                    compute_term,
                    nvl(r_sum_exception,0) pl_r_sum_exception,
                    nvl(trd_sum_exception,0) pl_trd_sum_exception,
                    nvl(rr_sum_exception,0) rr_sum_exception
                 from temp_stockinvplcheckexception
            """
        finalDf = self.sparkSession.sql(sql)
        self._cal_detail(finalDf, enddate)
        self._stat_report(finalDf, enddate)

    def _cal_detail(self, finalDf, enddate):
        finalDfData = finalDf.where("""
                    pl_r_sum_exception>0  or pl_trd_sum_exception>0 or
                    rr_sum_exception>0
            """)
        save_data(self.sparkSession, self.adata, self.taget_detail_table, enddate, finalDfData)

    def _stat_report(self, df, enddate):
        allCount = df.count()
        finalDf = df.groupBy(["trade_id", "busi_date", "compute_term"]).agg(
            fn.sum("rr_sum_exception").alias("rr_sum_exception_uv"),
            fn.sum("pl_trd_sum_exception").alias("pl_trd_sum_exception_uv"),
            fn.sum("pl_r_sum_exception").alias("pl_r_sum_exception_uv"),
            (fn.sum("rr_sum_exception") / allCount).alias("rr_sum_exception_rate"),
            (fn.sum("pl_trd_sum_exception") / allCount).alias("pl_trd_sum_exception_rate"),
            (fn.sum("pl_r_sum_exception") / allCount).alias("pl_t_sum_exception_rate"))
        save_data(self.sparkSession, self.adata, self.taget_report_table, enddate, finalDf)
