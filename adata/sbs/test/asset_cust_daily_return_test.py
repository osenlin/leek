# coding=utf-8
from testbase import testBase
from leek.adata.sbs.asset_cust_daily_return import AssetCustDailyReturn


class ASCDHTest(testBase):
    def setUp(self):
        AssetCustDailyReturn.logLevel = 'debug'
        self.scdh = AssetCustDailyReturn(None)

    def test_init_data(self):
        self.scdh.init_data()

    def test_daily_compute(self):
        self.scdh.daily_compute("2017-03-15", "2017-03-16")

    def test_stock_asset_return(self):
        # double或者float 精度类型<0.1
        # a.exception_label或者b.exception_label,不能为1
        checkSql = """
            select a.trade_id, a.return-b.total_return diff ,a.exception_label,b.exception_label
            from adatatest.asset_cust_daily_return
            a inner join adatatest.stock_cust_daily_return b on
            a.trade_id=b.trade_id and a.busi_date=b.busi_date --and a.trade_id=14157
            where a.return-b.total_return>0.1 and a.exception_label=0 and b.exception_label=0
        """
        self.scdh.sparkSession.sql(checkSql)

        AssetSql = """
            SELECT * from adatatest.asset_cust_daily_return
            where trade_id=14157
            ORDER  by busi_date
        """
        self.scdh.sparkSession.sql(AssetSql)

        sqlstartReturn = """
            SELECT * from adatatest.stock_cust_daily_return
            where trade_id=14157
            ORDER  by busi_date
        """
        self.scdh.sparkSession.sql(sqlstartReturn)

        sqlCashFlow = """
            select busi_date,sum(capital_in) ci,sum(capital_out) co from fdata.stock_cash_flow
            where trade_id=14157
            GROUP  BY  busi_date
            ORDER  by busi_date
        """
        self.scdh.sparkSession.sql(sqlCashFlow)

        sqlCashFlowDetail = """
            select busi_date,trd_capital_type,sum(trd_cash_flow) co
            from odata.stock_cash_flow_detail
            where trade_id=14157
            GROUP  BY  busi_date,trd_capital_type
            ORDER  by busi_date
        """
        self.scdh.sparkSession.sql(sqlCashFlowDetail)

    def test_all(self):
        pass
