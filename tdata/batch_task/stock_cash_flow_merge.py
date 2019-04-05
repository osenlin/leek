# coding=utf-8

from leek.tdata.batch_task.base_job import LeekSparkJob
from leek.common.util import save_data

class StockCashFlowMerge(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.taget_table = self.conf['stock_cash_flow_merge_table']

    def init_data(self):
        print '本模块无需要初始化'

    def daily_compute(self, startdate=None, enddate=None):
        sql = """
            select
                trade_id,secu_acc_id,prd_no,
                concat_ws('-',collect_list(busi_flag_code)) busi_flag_code,
                concat_ws('-',collect_list(busi_flag_name)) busi_flag_name,
                sum(trd_qty) trd_qty,
                sum(case when trd_cash_flow >=0 then trd_qty else 0 end) pos_trd_qty,
                sum(case when trd_cash_flow <0 then trd_qty else 0 end) neg_trd_qty,
                sum(trd_cash_flow) trd_cash_flow,
                sum(case when trd_cash_flow >=0 then trd_cash_flow else 0 end) pos_cash_flow,
                sum(case when trd_cash_flow <0 then trd_cash_flow else 0 end) neg_cash_flow,
                sum(case when trd_capital_type='capital_in'
                         then trd_cash_flow else 0 end) capital_in,
                sum(case when trd_capital_type='capital_out'
                         then trd_cash_flow else 0 end) capital_out,
                sum(trd_cash_flow-trd_amt) trd_fee,
                max(cash_flow_modi_label) cash_flow_modi_label,
                sum(nvl(int_tax_in,0)) int_tax_in,
                sum(nvl(int_tax_out,0)) int_tax_out,
                trd_type,
                busi_date
            from {2}.{3}
            where busi_date >= '{0}' and busi_date <='{1}'
            group by trade_id,secu_acc_id,prd_no,trd_type,busi_date
            """.format(startdate, enddate, self.odata, self.conf['cash_flow_table'])
        df = self.sparkSession.sql(sql)
        self.batch_drop_partition(startdate, enddate, self.tdata, self.taget_table)
        save_data(self.sparkSession, self.tdata, self.taget_table, None, df,
                  defaultDropPartition=False)
