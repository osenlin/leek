# coding=utf-8
from leek.fdata.dc.leekjob import LeekSparkJob


class StockAccountCheck(LeekSparkJob):
    """
    账目核对计算模块
    """

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cash_flow = self.conf['stock_cash_flow_merge_table']

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
                    neg_cash_flow	double comment '负交易金额',
                    capital_in double ,
                    capital_out double,
                    int_tax_in double,
                    int_tax_out double,
                    busi_flag_code	string comment '业务代码',
                    return	double comment '收益',
                    return_rate	double comment '收益率',
                    qty_exception	bigint comment '持仓量核对异常',
                    return_rate_exception	bigint comment '收益率核对异常',
                    trd_type	string comment '交易类型'
                    )
                    comment '账目核对模块'
                    partitioned by (busi_date   string comment '交易日期')
                    STORED AS ORC
        """.format(self.fdata, self.conf['check_data_table'])
        self.sparkSession.sql(sql)

    def daily_compute(self, startdate=None, enddate=None):
        endDate = enddate
        beginDate = startdate

        self.sparkSession.sql(
            " alter table {1}.{2} add if not exists  partition(busi_date= '{0}') "
            .format(endDate, self.fdata, self.conf['check_data_table']))

        checkSqlTmp = """
            select
                ft.trade_id,ft.secu_acc_id,ft.prd_no,
                ft.pre_qty,
                ft.trd_qty,
                ft.now_qty,
                ft.pre_mkt_val,
                ft.now_mkt_val,
                ft.trd_cash_flow,
                ft.pos_cash_flow,
                ft.neg_cash_flow,
                capital_in,
                capital_out,
                int_tax_in,
                int_tax_out,
                ft.busi_flag_code,
                case when ft.prd_no='0.0' then 0 else ft.return end return,
                case when ft.prd_no='0.0' then 0 else ft.return_rate end return_rate,
                case when ft.prd_no='0.0' then 0 else ft.qty_exception end qty_exception,
                case when ft.prd_no='0.0' or rg.prd_no is null or (ft.return_rate>{6}
                     and ft.return_rate<{7}) then 0 else 1 end return_rate_exception,
                trd_type
            from (
                select nvl(oa.trade_id,fb.trade_id) trade_id,
                nvl(oa.secu_acc_id,fb.secu_acc_id) secu_acc_id,
                nvl(oa.prd_no,fb.prd_no) prd_no,
                nvl(pre_qty,0) pre_qty,
                nvl(trd_qty,0) trd_qty,
                nvl(now_qty,0) now_qty,
                nvl(pre_mkt_val,0) pre_mkt_val,
                nvl(now_mkt_val,0) now_mkt_val,
                nvl(trd_cash_flow,0) trd_cash_flow,
                nvl(pos_cash_flow,0) pos_cash_flow,
                nvl(neg_cash_flow,0) neg_cash_flow,
                nvl(capital_in,0) capital_in,
                nvl(capital_out,0) capital_out,
                nvl(int_tax_in,0) int_tax_in,
                nvl(int_tax_out,0) int_tax_out,
                busi_flag_code,
                nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0) return,
                {0} return_rate,
                (case when nvl(pre_qty,0)+cast(nvl(trd_qty,0) as bigint)>= nvl(now_qty,0)
                and  nvl(pre_qty,0)+cast(nvl(trd_qty,0)  as bigint)<nvl(now_qty,0)+cast(1 as bigint)
                then 0 else 1 end)  qty_exception,
                0 return_rate_exception,
                '{5}' trd_type
                from (
                    select trade_id,secu_acc_id,prd_no,
                    sum(case when busi_date='{2}' then {1} else cast(0 as bigint) end) pre_qty,
                    sum(case when busi_date='{3}' then {1} else cast(0 as bigint) end) now_qty,
                    sum(case when busi_date='{2}' then mkt_val
                             else cast(0 as bigint) end) pre_mkt_val,
                    sum(case when busi_date='{3}' then mkt_val
                             else cast(0 as bigint) end) now_mkt_val
                    from {4}
                    -- pro='0.0'现金流
                    where busi_date>='{2}' and busi_date<='{3}' --and prd_no<>'0.0'
                    group by trade_id,secu_acc_id,prd_no
                ) oa full join (select * from
                    {8}
                    where busi_date='{3}' and trd_type='{5}') fb
                    on oa.trade_id=fb.trade_id
                    and oa.secu_acc_id=fb.secu_acc_id
                    and oa.prd_no=fb.prd_no
            ) ft
            left outer join (select * from {9} where busi_date= '{3}' )  rg
            on ft.prd_no=rg.prd_no
        """

        longCheckSql = checkSqlTmp.format(
            """(nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0))/
               (nvl(pos_cash_flow,0)+nvl(pre_mkt_val,0))""",
            "qty", beginDate, endDate, "{0}.{1}".format(self.odata, self.conf['asset_table']),
            "long_related", "long_lower_limit", "long_upper_limit",
            "{0}.{1}".format(self.fdata, self.stock_cash_flow),
            "{0}.{1}".format(self.fdata, self.conf['stock_return_rate_range_table']))
        shortCheckSql = checkSqlTmp.format(
            """(nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0))/
                (nvl(pos_cash_flow,0)-nvl(now_mkt_val,0))""",
            "liab_qty", beginDate, endDate, "{0}.{1}".format(self.odata, self.conf['debt_table']),
            "short_related", "short_lower_limit", "short_upper_limit",
            "{0}.{1}".format(self.fdata, self.stock_cash_flow),
            "{0}.{1}".format(self.fdata, self.conf['stock_return_rate_range_table']))

        longTempTable = "longcheck_temp"
        dfLong = self.sparkSession.sql(longCheckSql)
        dfLong.repartition(20).createOrReplaceTempView(longTempTable)

        insertSql = """
            insert {0} table {1} partition(busi_date='{2}')
            select * from {3}
        """
        self.sparkSession.sql(insertSql.format("overwrite", "{0}.{1}".format(self.fdata, self.conf[
            'check_data_table']), endDate, longTempTable))
        self.sparkSession.catalog.dropTempView(longTempTable)

        shortTempTable = "shortcheck_temp"
        dfShort = self.sparkSession.sql(shortCheckSql)
        dfShort.repartition(20).createOrReplaceTempView(shortTempTable)

        self.sparkSession.sql(
            insertSql.format("into", "{0}.{1}".format(self.fdata, self.conf['check_data_table']),
                             endDate, shortTempTable))
        self.sparkSession.catalog.dropTempView(shortTempTable)
        self.sparkSession.sql("refresh table %s.%s" % (self.fdata, self.conf['check_data_table']))


if __name__ == '__main__':
    StockAccountCheck.logLevel = 'debug'
    StockAccountCheck("11").daily_compute("2017-04-10", "2017-04-10")
