# coding=utf-8
from leek.tdata.batch_task.base_job import LeekSparkJob
from leek.common.util import save_data


class StockCustDailyReturn(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_daily_return = self.conf['stock_cust_daily_return_table']
        self.stock_daily_check_data = self.conf["check_data_table"]

    def init_data(self):
        print '本模块不需要初始化'

    def daily_compute(self, startdate=None, enddate=None):
        sql = """
            select
                trade_id,pre_asset_val,pre_debt_val,now_asset_val,
                now_debt_val,capital_in,capital_out,
                nvl(qty_exception, 0) exception_label,
                long_return,short_return,total_return,
                nvl(long_return/{3},0.0) long_return_rate,
                nvl(short_return/{3},0.0) short_return_rate,
                nvl(total_return/{3},0.0) total_return_rate,
                int_tax_in,
                int_tax_out,
                busi_date
            from (
                select
                    trade_id,
                    sum(case when trd_type='long_related'
                             then pre_mkt_val else 0 end ) pre_asset_val,
                    sum(case when trd_type='short_related'
                             then pre_mkt_val else 0 end ) pre_debt_val,
                    sum(case when trd_type='long_related'
                             then now_mkt_val else 0 end ) now_asset_val,
                    sum(case when trd_type='short_related'
                             then now_mkt_val else 0 end ) now_debt_val,
                    sum(capital_in) capital_in,
                    sum(capital_out) capital_out,
                    max(qty_exception) qty_exception,
                    max(return_rate_exception) return_rate_exception,
                    sum(case when trd_type='long_related'
                        and qty_exception=0 then return else 0 end ) long_return,
                    sum(case when trd_type='short_related'
                        and qty_exception=0 then return else 0 end ) short_return,
                    sum(case when qty_exception=0
                        then return else 0 end) total_return,
                    sum(nvl(int_tax_in,0)) int_tax_in,
                    sum(nvl(int_tax_out,0)) int_tax_out,
                    busi_date
                from {1}.{2}
                where busi_date >='{0}' and busi_date <='{4}'
                group by trade_id,busi_date
            ) a
        """

        selectSql = sql.format(startdate, self.tdata, self.stock_daily_check_data, """
                    greatest(greatest(pre_asset_val+pre_debt_val,-1 * pre_debt_val),
                             greatest(pre_asset_val+pre_debt_val,-1 * pre_debt_val)
                             +capital_in+int_tax_in,
                             greatest(pre_asset_val+pre_debt_val,-1 * pre_debt_val)
                             +capital_in+capital_out+int_tax_in+int_tax_out)
                             """, enddate)
        dfLong = self.sparkSession.sql(selectSql)
        self.batch_drop_partition(startdate, enddate, self.tdata, self.stock_cust_daily_return)
        save_data(self.sparkSession, self.tdata, self.stock_cust_daily_return, None, dfLong,
                  defaultDropPartition=False)
