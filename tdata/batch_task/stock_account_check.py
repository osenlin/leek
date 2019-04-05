# coding=utf-8
from leek.tdata.batch_task.base_job import LeekSparkJob
from leek.common.util import save_data
from pyspark.storagelevel import StorageLevel

class StockAccountCheck(LeekSparkJob):
    """
    账目核对计算模块
    """

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.cash_flow = self.conf['stock_cash_flow_merge_table']
        self.return_range = self.conf['stock_return_rate_range_table']
        self.longTempTable = "stock_account_longTemp"
        self.shortTempTable = "stock_account_shortTemp"
        self.check_data = self.conf['check_data_table']

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
        """.format(self.tdata, self.conf['check_data_table'])
        print sql

    def daily_compute(self, startdate=None, enddate=None):
        self.get_base(startdate, enddate)
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
                ft.busi_flag_code,
                ft.int_tax_in,
                ft.int_tax_out,
                case when ft.prd_no='0.0' then 0 else ft.return end return,
                case when ft.prd_no='0.0' then 0 else ft.return_rate end return_rate,
                case when ft.prd_no='0.0' then 0 else ft.qty_exception end qty_exception,
                case when ft.prd_no='0.0' or rg.prd_no is null or (ft.return_rate>{2}
                     and ft.return_rate<{3}) then 0 else 1 end return_rate_exception,
                ft.trd_type,
                ft.busi_date
            from {5} ft
            left outer join (
              select * from {4} where busi_date >= '{0}' and busi_date<='{1}' )  rg
            on ft.prd_no=rg.prd_no and ft.busi_date=rg.busi_date
        """
        longCheckSql = checkSqlTmp.format(
            startdate, enddate, "long_lower_limit", "long_upper_limit",
            "{0}.{1}".format(self.fdata, self.return_range), self.longTempTable)
        dfLong = self.sparkSession.sql(longCheckSql).persist(StorageLevel.DISK_ONLY)
        shortCheckSql = checkSqlTmp.format(
            startdate, enddate, "short_lower_limit", "short_upper_limit",
            "{0}.{1}".format(self.fdata, self.return_range), self.shortTempTable)
        dfShort = self.sparkSession.sql(shortCheckSql).persist(StorageLevel.DISK_ONLY)
        self.batch_drop_partition(startdate, enddate, self.tdata, self.check_data)
        save_data(self.sparkSession, self.tdata, self.check_data, None,dfLong,
                  defaultDropPartition=False)
        save_data(self.sparkSession, self.tdata, self.check_data, None, dfShort,
                  defaultDropPartition=False)

    def get_base(self, beginDate, endDate):
        self.get_now_date(beginDate, endDate)
        sqlTmp = """
            select
                nvl(oa.trade_id,oaa.trade_id) trade_id,
                nvl(oa.secu_acc_id,oaa.secu_acc_id) secu_acc_id,
                nvl(oa.prd_no,oaa.prd_no) prd_no,
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
                nvl(busi_flag_code,'') busi_flag_code,
                nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0) return,
                {0} return_rate,
                (case when nvl(pre_qty,0)+cast(nvl(trd_qty,0) as bigint)>= nvl(now_qty,0)
                and  nvl(pre_qty,0)+cast(nvl(trd_qty,0)  as bigint)<nvl(now_qty,0)+cast(1 as bigint)
                then 0 else 1 end)  qty_exception,
                0 return_rate_exception,
                '{4}' trd_type,
                nvl(oa.busi_date,lastdate) busi_date
            from {5} oa full outer join (
                  select  trade_id,secu_acc_id,prd_no,{6} pre_qty,mkt_val pre_mkt_val,
                       busi_date,lastdate
                  from {3} st inner JOIN (
                  select busi_date bdate,lead(busi_date) over(order by busi_date) lastdate
                  from {7}
                ) b on st.busi_date=b.bdate
                  where busi_date>='{1}' and busi_date<'{2}'
              ) oaa on oa.trade_id=oaa.trade_id and oa.secu_acc_id=oaa.secu_acc_id
              and oa.prd_no=oaa.prd_no and oa.startdate=oaa.busi_date
            -- where (oa.trade_id is not null and oaa.trade_id is not null) or
            --       (oa.busi_date='{1}' and oaa.trade_id is null)
        """
        longSql = sqlTmp.format(
            """(nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0))/
               (nvl(pos_cash_flow,0)+nvl(pre_mkt_val,0))""",
            beginDate, endDate, "{0}.{1}".format(self.odata, self.conf['asset_table']),
            "long_related", "check_long_now_temp_table", "qty",
            "{0}.{1}".format(self.odata, self.trading_calendar)
        )
        self.sparkSession.sql(longSql).\
            persist(StorageLevel.DISK_ONLY).\
            createOrReplaceTempView(self.longTempTable)
        shortSql = sqlTmp.format(
            """(nvl(now_mkt_val,0)-nvl(pre_mkt_val,0)-nvl(trd_cash_flow,0))/
                (nvl(pos_cash_flow,0)-nvl(now_mkt_val,0))""",
            beginDate, endDate, "{0}.{1}".format(self.odata, self.conf['debt_table']),
            "short_related", "check_short_now_temp_table", "liab_qty",
            "{0}.{1}".format(self.odata, self.trading_calendar)
        )
        self.sparkSession.sql(shortSql).\
            persist(StorageLevel.DISK_ONLY).\
            createOrReplaceTempView(self.shortTempTable)

    def get_now_date(self, beginDate, endDate):
        nowTemp="""
            select 
              nvl(oa.trade_id,fb.trade_id) trade_id,
              nvl(oa.secu_acc_id,fb.secu_acc_id) secu_acc_id,
              nvl(oa.prd_no,fb.prd_no) prd_no,
              nvl(oa.now_qty,0) now_qty,
              nvl(oa.now_mkt_val,0) now_mkt_val,
              nvl(oa.busi_date,fb.busi_date) busi_date,
              nvl(oa.startdate,fb.startdate) startdate,
              nvl(trd_qty,0) trd_qty,
              nvl(trd_cash_flow,0) trd_cash_flow,
              nvl(pos_cash_flow,0) pos_cash_flow,
              nvl(neg_cash_flow,0) neg_cash_flow,
              nvl(capital_in,0) capital_in,
              nvl(capital_out,0) capital_out,
              nvl(int_tax_in,0) int_tax_in,
              nvl(int_tax_out,0) int_tax_out,
              nvl(busi_flag_code,'') busi_flag_code
            from(
                select trade_id,secu_acc_id,prd_no,{5} now_qty,
                         mkt_val now_mkt_val,a.busi_date,startdate
                from {2} a inner JOIN (
                  select busi_date,lag(busi_date) over(order by busi_date) startdate
                  from {6}
                ) b on a.busi_date=b.busi_date
                where a.busi_date>='{0}' and a.busi_date<='{1}'
             ) oa full outer join (
                select * from {4}  st inner JOIN (
                  select busi_date bdate,lag(busi_date) over(order by busi_date) startdate
                  from {6}
                ) b on st.busi_date=b.bdate
                where st.busi_date>='{0}' and st.busi_date<='{1}' and trd_type='{3}'
             ) fb on oa.trade_id=fb.trade_id and oa.secu_acc_id=fb.secu_acc_id
              and oa.prd_no=fb.prd_no and oa.busi_date=fb.busi_date
        """
        longNowSql = nowTemp.format(beginDate, endDate,
                                    "{0}.{1}".format(self.odata, self.conf['asset_table']),
                                    "long_related", "{0}.{1}".format(self.tdata, self.cash_flow),
                                    "qty", "{0}.{1}".format(self.odata, self.trading_calendar))
        shortNowSql = nowTemp.format(beginDate, endDate,
                                     "{0}.{1}".format(self.odata, self.conf['debt_table']),
                                     "short_related", "{0}.{1}".format(self.tdata, self.cash_flow),
                                     "liab_qty",
                                     "{0}.{1}".format(self.odata, self.trading_calendar))
        self.sparkSession.sql(longNowSql).\
            persist(StorageLevel.DISK_ONLY).\
            createOrReplaceTempView("check_long_now_temp_table")
        self.sparkSession.sql(shortNowSql).\
            persist(StorageLevel.DISK_ONLY).\
            createOrReplaceTempView("check_short_now_temp_table")