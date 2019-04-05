# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, get_date


class AssetDrCheckExceptionReport(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.asset_dr_check_exception_report = self.conf['asset_dr_check_exception_report_table']
        self.asset_dr_check_exception_data = self.conf['asset_dr_check_exception_data_table']
        self.asset_cust_daily_return = self.conf['asset_cust_daily_return_table']

    def init_data(self):
        print 'init stock_ac_check_exception_report'

    def daily_compute(self, startdate=None, enddate=None):
        sqlAllCount = """
                select count(distinct trade_id) all_num from {1}.{2}
                where busi_date= '{0}'
        """.format(enddate, self.adata, self.asset_cust_daily_return)
        allNum = self.sparkSession.sql(sqlAllCount).first().all_num
        sqlTmp = """
            select busi_date,
                sum(ac_exception_label) ac_exception_uv,
                cast(sum(ac_exception_label)*1.0/{3} as double) ac_exception_rate,
                sum(pos_neg_exception) pos_neg_exception_uv,
                cast(sum(pos_neg_exception)*1.0/{3} as double) pos_neg_exception_rate,
                sum(rr_outlier_exception) rr_outlier_exception_uv,
                cast(sum(rr_outlier_exception)*1.0/{3} as double) rr_outlier_exception_rate,
                sum(rr_sp_exception) rr_sp_exception_uv,
                cast(sum(rr_sp_exception)*1.0/{3} as double) rr_sp_exception_rate
            from  {1}.{2}
            where busi_date= '{0}'
            group by busi_date
        """.format(enddate, self.adata, self.asset_dr_check_exception_data, allNum)
        df = self.sparkSession.sql(sqlTmp).repartition(5)
        save_data(self.sparkSession, self.adata, self.asset_dr_check_exception_report, enddate, df)
