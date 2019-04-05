# coding=utf-8

from leek.fdata.hkdc.test.testbase import testBase
from leek.common.util import setup_spark
from leek.fdata.hkdc.stock_cash_flow_merge import StockCashFlowMerge


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
