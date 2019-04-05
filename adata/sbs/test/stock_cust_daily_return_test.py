from testbase import testBase
from leek.adata.sbs.stock_cust_daily_return import StockCustDailyReturn


class SCDHTest(testBase):

    def setUp(self):
        StockCustDailyReturn.logLevel = 'debug'
        self.scdh = StockCustDailyReturn(None)
        self.spark = testBase.setupSpark(None, "yarn", "app_cash_flow_merge")

    def test_init_data(self):
        self.scdh.init_data()

    def test_daily_compute(self):
        self.scdh.daily_compute(None, "2017-03-16")

    def test_check_1(self):
        sql = """
            SELECT * from adatatest.stock_cust_daily_return
            where short_return_rate>1 or long_return_rate>1 or total_return_rate>1
        """
        self.spark.sql(sql)

    def test_rule_sql(self):
        sql = """
        select * from (
        select trade_id,busi_date,abs(short_return+long_return-total_return) diff_total,
        abs(long_return_rate+short_return_rate-total_return_rate) diff_return_rate
        from adata.stock_cust_daily_return
        ) a
        where diff_total>0.1 and diff_return_rate>0.1
        """
        self.assertEquals(self.spark.sql(sql).count(), 0)
