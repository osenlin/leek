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

    def test_sr_exception(self):
        sql = """
            select * from adata.stock_oc_check_exception_data
            where c_return!=o_return and sr_equal_exception=1
        """
        """
        stock_oc_check_exception_data.trade_id	68639
        stock_oc_check_exception_data.secu_acc_id
        stock_oc_check_exception_data.prd_no	1.603658
        stock_oc_check_exception_data.o_return	966
        stock_oc_check_exception_data.o_exception_label	0
        stock_oc_check_exception_data.c_return	1486
        stock_oc_check_exception_data.c_exception_label	0
        stock_oc_check_exception_data.lr_equal_exception	0
        stock_oc_check_exception_data.lmv_euqal_exception	0
        stock_oc_check_exception_data.sr_equal_exception	1
        stock_oc_check_exception_data.smv_equal_exception	0
        stock_oc_check_exception_data.busi_date	2017-04-13
        """
        sql = """
        select *
        from fdata.stock_close_o_trade_short_data
        where busi_date='2017-04-13' and trade_id='68639' and prd_no='1.603658'
        """
        self.spark.sql(sql)

        sql = """
        select *
        from fdata.stock_close_c_trade_short_data
        where busi_date='2017-04-13' and trade_id='68639' and prd_no='1.603658'
        """
        self.spark.sql(sql)

        sql = """
        select *
        from fdata.stock_unclose_c_trade_short_data
        where busi_date='2017-04-13' and trade_id='68639' and prd_no='1.603658'
        """
        self.spark.sql(sql)

        sql = """
            select * from fdata.stock_cash_flow
            where busi_date>='2017-04-07' and busi_date<='2017-04-13'
            and trade_id='68639' and prd_no='1.603658'
            order by busi_date
        """

        self.spark.sql(sql)

    def test_lr_exception(self):
        sql = """
        select * from adata.stock_oc_check_exception_data
        where c_return!=o_return and lr_equal_exception=1
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
        sql = """
            select *
            from fdata.stock_close_o_trade_long_data
            where busi_date='2017-03-27' and trade_id='83995' and prd_no='2.000510'
        """
        self.spark.sql(sql)

        sql = """
            select *
            from fdata.stock_close_c_trade_long_data
            where busi_date='2017-03-27' and trade_id='83995' and prd_no='2.000510'
        """
        self.spark.sql(sql)

        sql = """
            select *
            from fdata.stock_cash_flow
            where busi_date>='2017-03-21' and busi_date<='2017-03-27'
               and trade_id='83995' and prd_no='2.000510'
        """
        self.spark.sql(sql)

        sql = """
            select * from odata.stock_asset_holding
             where busi_date>='2017-03-21' and busi_date<='2017-03-27'
               and trade_id='83995' and prd_no='2.000510'
        """
        self.spark.sql(sql)
        sql = """
                   select * from fdata.stock_unclose_c_trade_long_cal
                    where busi_date>='2017-03-21' and busi_date<='2017-03-27'
                      and trade_id='83995' and prd_no='2.000510'
               """
        self.spark.sql(sql)
