# coding=utf-8
from pyspark import SparkConf, SparkContext

from testbase import testBase
from leek.adata.ssid.stock_tradebyday_data import StockTradeByDayData
from pyspark.sql import functions as Fn, SparkSession


class SCDHTest(testBase):

    def setUp(self):
        testBase.logLevel = 'debug'
        conf = SparkConf()
        self.spark = self.setupSpark(conf, 'locl', 'hl')

    def test_lr_exception(self):
        sql = """
        select * from adata.stock_otbd_check_exception_data
        where tbd_return!=o_return
        """
        """
        stock_oc_check_exception_data.trade_id	83995
        stock_oc_check_exception_data.secu_acc_id
        stock_oc_check_exception_data.prd_no	2.000510
        stock_oc_check_exception_data.o_return	-2309.67
        stock_oc_check_exception_data.o_exception_label	1
        stock_oc_check_exception_data.c_return	14535.33
        stock_oc_check_exception_data.c_exception_label	1
        stock_oc_check_exception_data.lr_equal_exception	1
        stock_oc_check_exception_data.lmv_euqal_exception	0
        stock_oc_check_exception_data.sr_equal_exception	0
        stock_oc_check_exception_data.smv_equal_exception	0
        stock_oc_check_exception_data.busi_date	2017-03-27
        :return:
        """
