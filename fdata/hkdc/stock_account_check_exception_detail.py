# coding=utf-8

from leek.fdata.hkdc.leekjob import LeekSparkJob


class StockAccountCheckExceptionDetail(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.fdata = self.conf['feature_database']
        self.check_data_exception_table = self.conf['hk_check_data_exception_table']
        self.check_table = self.conf['hk_check_data_table']

    def init_data(self):
        sql = """
            create table if not exists {0}.{1} (
                trade_id	string comment '客户代码',
                secu_acc_id	string comment '股票账户',
                prd_no	string comment '资产代码',
                pre_qty	bigint comment '前一交易日持仓（或负债）数量',
                trd_qty	bigint comment '交易数量',
                now_qty	bigint comment '当前持仓（或负债）数量',
                pre_mkt_val	double comment '前一交易日持仓（或负债）市值',
                now_mkt_val	double comment '当前持仓（或负债）市值',
                trd_cash_flow	double comment '交易金额',
                pos_cash_flow	double comment '正交易金额',
                capital_in double,
                capital_out double,
                busi_flag_code	string comment '业务代码',
                exception_type	string comment '异常类型',
                trd_type	string comment '交易类型'
                )
                comment '持仓核对和收益率核对异常明细数据'
                partitioned by (busi_date   string comment '交易日期')
                STORED AS ORC
        """.format(self.fdata, self.check_data_exception_table)
        self.sparkSession.sql(sql)

    def daily_compute(self, startdate=None, enddate=None):
        self.sparkSession.sql(
            " alter table {1}.{2} add if not exists  partition(busi_date= '{0}') "
            .format(enddate, self.fdata, self.check_data_exception_table))
        sqlTmp = """
            select  trade_id,secu_acc_id,prd_no,pre_qty,trd_qty,now_qty,pre_mkt_val,
                    now_mkt_val,trd_cash_flow,pos_cash_flow,capital_in,capital_out,busi_flag_code,
                    case when qty_exception =1 and return_rate_exception=0 then 'qty'
                        when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                        else 'both' end exception_type,
                    trd_type
            from {1}.{2}
            where  busi_date='{0}' and (qty_exception<>0 or return_rate_exception<>0)
        """.format(enddate, self.fdata, self.check_table)
        df = self.sparkSession.sql(sqlTmp)
        temptable = "check_exception_detail_temp"
        df.repartition(20).createOrReplaceTempView(temptable)
        self.sparkSession.sql("""
            insert overwrite table {1}.{2} partition(busi_date='{0}')
            select * from {3}
        """.format(enddate, self.fdata, self.check_data_exception_table, temptable))
        self.sparkSession.sql(
            "refresh table %s.%s" % (self.fdata, self.check_data_exception_table))
        self.sparkSession.catalog.dropTempView(temptable)


if __name__ == '__main__':
    StockAccountCheckExceptionDetail.logLevel = 'debug'
    StockAccountCheckExceptionDetail("11").daily_compute("2017-04-10", "2017-04-10")
