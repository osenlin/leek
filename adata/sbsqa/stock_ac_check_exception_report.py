# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data


class StockAcCheckExceptionReport(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_ac_check_exception_report = self.conf['stock_ac_check_exception_report_table']
        self.stock_daily_check_data = self.conf['check_data_table']

    def init_data(self):
        print 'init stock_ac_check_exception_report'

    def daily_compute(self, startdate=None, enddate=None):
        sqlTmp = """
            select
                exception_pv,exception_uv,
                exception_pv/b.all_num exception_pv_rate,
                exception_uv/b.all_user exception_uv_rate,
                max_return,min_return,
                max_return_rate,min_return_rate,
                exception_type,
                busi_date
            from (
                select busi_date,
                    count(1) exception_pv,
                    count(distinct trade_id) exception_uv,
                    max(return) max_return,
                    min(return) min_return,
                    max(return_rate) max_return_rate,
                    min(return_rate) min_return_rate,
                    case when qty_exception =1 and return_rate_exception=0 then 'qty'
                         when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                         else 'both' end exception_type
                from {1}.{2}
                where  busi_date= '{0}' and (qty_exception<>0 or return_rate_exception<>0)
                group by case when qty_exception =1 and return_rate_exception=0 then 'qty'
                              when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                              else 'both' end,busi_date
            ) a cross join (
                select count(1) all_num,count(distinct trade_id) all_user
                from {1}.{2}
                where busi_date= '{0}'
            ) b
        """.format(enddate, self.fdata, self.conf['check_data_table'])
        df = self.sparkSession.sql(sqlTmp).repartition(5)
        save_data(self.sparkSession, self.adata, self.stock_ac_check_exception_report, enddate, df)
