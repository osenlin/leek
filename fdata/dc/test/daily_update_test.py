# coding=utf-8

from leek.fdata.dc.test.testbase import testBase
from leek.fdata.dc.daily_update import _run

from leek.fdata.dc.leekjob import LeekSparkJob
from leek.common.util import get_trading_day, get_config, get_date, setup_spark
from leek.fdata.dc.stock_cash_flow_merge import StockCashFlowMerge
from leek.fdata.dc.stock_account_check import StockAccountCheck


class DailyUpdateTest(testBase):

    def setUp(self):
        StockCashFlowMerge.logLevel = 'debug'
        self.stModel = StockCashFlowMerge("11")

    def test_run(self):
        self.stModel.daily_compute("", "")

    def test_evl(self):
        spark = setup_spark({})
        eval('{0}({1}).{2}({3},{4})'.format("StockCashFlowMerge", spark, "daily_compute",
                                            "2017-03-14", "2017-03-15"))
