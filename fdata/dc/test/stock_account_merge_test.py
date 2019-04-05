# coding=utf-8

from leek.fdata.dc.test.testbase import testBase
from leek.fdata.dc.daily_update import _run

from leek.fdata.dc.leekjob import LeekSparkJob
from leek.common.util import get_trading_day, get_config, get_date, setup_spark
from leek.fdata.dc.stock_cash_flow_merge import StockCashFlowMerge
from leek.fdata.dc.stock_account_check import StockAccountCheck


class stockAccountMerge_test(testBase):

    def test_run(self):
        StockCashFlowMerge({})

    def test_evl(self):
        spark = setup_spark({})
        eval('{0}({1}).{2}({3},{4})'.format("StockCashFlowMerge", spark, "daily_compute",
                                            "2017-03-14", "2017-03-15"))

    def test_func(self):
        self.sparkSession.createDataFrame().join()
