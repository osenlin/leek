# coding=utf-8

from leek.fdata.hkdc.leekjob import LeekSparkJob
from leek.common.util import save_data


class StockCashFlowMerge(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.cash_detail = self.conf['hk_cash_flow_table']
        self.cash_flow_merge = self.conf['hk_stock_cash_flow_merge_table']

    def init_data(self):
        sql = """
            create table if not exists {0}.{1} (
                trade_id string comment '客户代码',
                secu_acc_id   string comment '股票账户',
                prd_no   string comment '资产代码',
                busi_flag_code   string comment '业务代码',
                busi_flag_name   string comment '业务名称',
                trd_qty   bigint comment '交易数量',
                pos_trd_qty   bigint comment '正交易数量',
                neg_trd_qty   bigint comment '负交易数量',
                trd_cash_flow   double comment '交易金额',
                pos_cash_flow   double comment '正交易金额',
                neg_cash_flow   double comment '负交易金额',
                capital_in double comment '资产流入额',
                capital_out double comment '资产流出额',
                trd_fee   double comment '交易费用',
                int_tax_in double ,
                int_tax_out double ,
                cash_flow_modi_label   double comment '现金流是否修正',
                trd_type   string comment '交易类型'
                )
                comment '现金流合并模块'
                partitioned by (busi_date   string comment '交易日期')
                STORED AS ORC
        """.format(self.fdata, self.cash_flow_merge)
        print sql

    def daily_compute(self, startdate=None, enddate=None):
        df = self.sparkSession.sql("""
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
                sum(nvl(int_tax_in,0)) int_tax_in,
                sum(nvl(int_tax_out,0)) int_tax_out,
                max(cash_flow_modi_label) cash_flow_modi_label,
                trd_type,
                max(ignore_rr_check) ignore_rr_check,
                '{0}' busi_date
            from {1}.{2}
            where busi_date = '{0}'
            group by trade_id,secu_acc_id,prd_no,trd_type
            """.format(enddate, self.odata, self.cash_detail))
        save_data(self.sparkSession, self.fdata, self.cash_flow_merge,
                  enddate, df)


if __name__ == '__main__':
    StockCashFlowMerge.logLevel = 'debug'
    StockCashFlowMerge("11").daily_compute("2017-04-10", "2017-04-10")
