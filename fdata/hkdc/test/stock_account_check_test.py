# coding=utf-8

from leek.fdata.hkdc.test.testbase import testBase
from leek.fdata.hkdc.stock_account_check import StockAccountCheck


class stockAccountChecktest(testBase):

    def setUp(self):
        StockAccountCheck.logLevel='debug'
        self.scdh=StockAccountCheck(None)

    def test_func(self):
        self.scdh.daily_compute("2017-03-17","2017-03-18")