# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data


class StockAcCheckException(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_ac_check_exception_data = self.conf['stock_ac_check_exception_data_table']
        self.stock_daily_check_data = self.conf['check_data_table']

    def init_data(self):
        print 'init stock_ac_check_exception_data'

    def daily_compute(self, startdate=None, enddate=None):
        sql = """
            select trade_id,
                    secu_acc_id,
                    prd_no,
                    busi_date,
                    pre_qty,
                    trd_qty,
                    now_qty,
                    pre_mkt_val,
                    now_mkt_val,
                    trd_cash_flow,
                    pos_cash_flow,
                    capital_in,
                    capital_out,
                    nvl(busi_flag_code,'') busi_flag_code,
            case when qty_exception>0 and  return_rate_exception>0 then 'both'
                when qty_exception>0 then 'qty'
                when return_rate_exception>0 then 'return_rate' end exception_type,
            trd_type
            from {1}.{2}
            where busi_date='{0}' and (qty_exception>0 or return_rate_exception>0)
        """
        sqlCmd = sql.format(enddate, self.fdata, self.stock_daily_check_data)
        df = self.sparkSession.sql(sqlCmd).repartition(5)
        save_data(self.sparkSession, self.adata, self.stock_ac_check_exception_data, enddate, df)
