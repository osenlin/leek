# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data


class StockCustDailyReturn(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_daily_return = self.conf['stock_cust_daily_return_table']
        self.stock_daily_check_data = self.conf["check_data_table"]

    def init_data(self):
        sql = """
            create table if not exists {0}.{1} (
                trade_id string,
                pre_asset_val double,
                pre_debt_val double,
                now_asset_val double,
                now_debt_val double,
                capital_in double,
                capital_out double,
                exception_label bigint,
                long_return double,
                short_return double,
                total_return double,
                long_return_rate double,
                short_return_rate double,
                total_return_rate double,
                int_tax_in double,
                int_tax_out double
            )
            comment '记录客户每日在股票市场上的持仓和每支股票的收益情况'
            partitioned by (busi_date   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.stock_cust_daily_return)
        print sql  # self.sparkSession.sql(sql)

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
                '{0}' busi_date
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
                    sum(int_tax_in) int_tax_in,
                    sum(int_tax_out) int_tax_out
                from {1}.{2}
                where busi_date='{0}'
                group by trade_id,busi_date
            ) a
        """

        selectSql = sql.format(enddate, self.fdata, self.stock_daily_check_data, """
                    greatest(greatest(pre_asset_val+pre_debt_val,-1 * pre_debt_val),
                             greatest(pre_asset_val+pre_debt_val,-1 * pre_debt_val)+capital_in+
                             int_tax_in,
                             greatest(pre_asset_val+pre_debt_val,-1 * pre_debt_val)+capital_in+
                             capital_out+int_tax_in+int_tax_out)
                             """)
        dfLong = self.sparkSession.sql(selectSql).repartition(20)
        save_data(self.sparkSession, self.adata, self.stock_cust_daily_return, enddate, dfLong)
