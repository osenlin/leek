# coding=utf-8

from leek.fdata.dc.test.testbase import testBase
from leek.fdata.dc.daily_update import _run

from leek.fdata.dc.leekjob import LeekSparkJob
from leek.common.util import get_trading_day, get_config, get_date, setup_spark
from leek.fdata.dc.stock_cash_flow_merge import StockCashFlowMerge
from leek.fdata.dc.stock_account_check import StockAccountCheck


class stockAccountChecktest(testBase):

    def setUp(self):
        StockAccountCheck.logLevel='debug'
        self.scdh=StockAccountCheck(None)

    def test_func(self):
        self.scdh.daily_compute("2017-03-17","2017-03-18")