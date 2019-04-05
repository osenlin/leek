from testbase import testBase
from leek.adata.sbs.stock_cust_daily_holding import StockCustDailyHolding


class SCDHTest(testBase):

    def setUp(self):
        StockCustDailyHolding.logLevel = 'debug'
        self.scdh = StockCustDailyHolding(None)

    def test_init_data(self):
        self.scdh.init_data()

    def test_daily_compute(self):
        self.scdh.daily_compute(None, "2017-03-16")
