# coding=utf-8

from leek.fdata.dc.test.testbase import testBase

from leek.tdata.batch_task.stock_account_check import StockAccountCheck


class stockAccountMerge_test(testBase):

    def setUp(self):
        StockAccountCheck.logLevel = 'debug'
        self.model = StockAccountCheck({})

    def test_run(self):
        self.model.get_now_date('2017-03-29', '2017-03-29')

    def test_run2(self):
        self.model.get_base('2017-03-16', '2017-04-01')
