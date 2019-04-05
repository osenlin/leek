# coding=utf-8

from leek.fdata.hkdc.test.testbase import testBase
from leek.common.util import setup_spark
from leek.fdata.hkdc.stock_cash_flow_merge import StockCashFlowMerge


class stockAccountMerge_test(testBase):

    def test_run(self):
        StockCashFlowMerge({})

    def test_evl(self):
        spark = setup_spark({})
        eval('{0}({1}).{2}({3},{4})'.format("StockCashFlowMerge", spark, "daily_compute",
                                            "2017-03-14", "2017-03-15"))

    def test_func(self):
        self.sparkSession.createDataFrame().join()
