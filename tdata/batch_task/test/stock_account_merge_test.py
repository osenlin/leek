# coding=utf-8

from leek.fdata.dc.test.testbase import testBase

from leek.tdata.batch_task.stock_account_check import StockAccountCheck


class stockAccountMerge_test(testBase):

    def setUp(self):
        StockAccountCheck.logLevel = 'debug'
        self.model = StockAccountCheck({})

    def test_run(self):
        self.model.daily_compute('2017-04-14', '2017-04-17')
