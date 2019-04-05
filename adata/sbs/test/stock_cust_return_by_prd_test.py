# -*- encoding: UTF-8 -*-
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

from testbase import testBase
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import *
from leek.adata.sbs.stock_cust_return_by_prd import StockCustReturnByPrd, _travel_row
import os
import sys


class SCDHTest(testBase):

    def setUp(self):
        StockCustReturnByPrd.logLevel = 'debug'
        os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        conf = SparkConf().setMaster("local").setAppName("hello")
        self.spark = SparkSession(SparkContext(conf=conf))
        self.scdh = StockCustReturnByPrd(self.spark)

    def tearDown(self):
        self.spark.stop()

    def test_mapPartition(self):
        df = self.spark.createDataFrame([{u"a": "1", u"b": "2"}, {u"a": "3", u"b": "4"}])
        df.persist()

        def t(ite, s):
            for ss in ite:
                yield {"t": ss.a, "s1": s}

        d = self.spark.createDataFrame(df.rdd.mapPartitions(lambda x: t(x, 1), 2))
        print d.withColumn("month", F.lit("hh")).show()

    def test_local_spark(self):
        doc = self.spark.createDataFrame([['a', 'b', 'c'], ['b', 'd', 'd']])
        print doc.show()
        print "successful!"

    def test_get_base_data(self):
        self.scdh._get_base_data("2017-03-16", "2017-03-18", 1, 5)

    def test_init_data(self):
        self.scdh.init_data()

    def test_daily_compute(self):
        self.scdh.daily_compute("2017-03-16", "2017-03-16")

    def test_check_1(self):
        sql = """
            SELECT * from adatatest.stock_cust_daily_return
            where short_return_rate>1 or long_return_rate>1 or total_return_rate>1
        """
        self.spark.sql(sql)

    # 收益率和收益率单测
    def test_travel_row(self):
        row1 = Row(
            trade_id=u'10036', prd_no=u'2.300262',
            prd_ind=u'\u73af\u5883\u4e0e\u8bbe\u65bd\u670d\u52a1',
            detail_list=[{u'return': u'5058', u'trd_type': u'long_related',
                          u'pos_cash_flow': u'3297265',
                          u'exception_label': u'0', u'pre_mkt_val': u'0.0',
                          u'now_mkt_val': u'3302324.0',
                          u'busi_date': u'2017-03-17',
                          u'neg_cash_flow': u'3297265'}])

        row2 = Row(trade_id=u'10178', prd_no=u'1.600729', prd_ind=u'unknown', detail_list=[
            {u'return': u'-18', u'trd_type': u'long_related', u'pos_cash_flow': u'13508',
             u'exception_label': u'0', u'pre_mkt_val': u'12000', u'now_mkt_val': u'13490.0',
             u'busi_date': u'2017-03-16', u'neg_cash_flow': u'13508'},
            {u'return': u'3816.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-255036.0', u'now_mkt_val': u'-251220.0',
             u'busi_date': u'2017-03-17', u'neg_cash_flow': u'0.0'},
            {u'return': u'-381', u'trd_type': u'short_related', u'pos_cash_flow': u'45000',
             u'exception_label': u'0', u'pre_mkt_val': u'3121', u'now_mkt_val': u'-255036.0',
             u'busi_date': u'2017-03-16', u'neg_cash_flow': u'0.0'},
            {u'return': u'-400.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'13490.0', u'now_mkt_val': u'13090.0',
             u'busi_date': u'2017-03-17', u'neg_cash_flow': u'0.0'},

        ])
        rowDict = _travel_row(row1, '2017-03-18')
        # 计算单个值的return，
        self.assertEqual(rowDict.get("return"), 5058, "return error!!!")
        # 计算单个值的return rate
        return_rate = 5058 * 1.0 / (3297265 + 3297265 + 0.0)
        print rowDict.get("return_rate"), return_rate
        self.assertEqual(rowDict.get("return_rate"), return_rate,
                         "first[{}],second[{}],msg[{}]".format(rowDict.get("return_rate"),
                                                               return_rate,
                                                               "return rate isn't expected"))
        # 模拟所有数据类型,计算return
        rowDict2 = _travel_row(row2, '2017-03-18')
        self.assertEquals(rowDict2.get("return"), -18 - 381 - 400.0 + 3816.0,
                          "return isn't expected")
        # 模拟所有数据类型,计算return_rate
        print rowDict2.get("return_rate"), (-18 - 381 - 400.0 + 3816.0) / (
            12000 + 13508 + 13508 + 251220.0 + 45000)
        self.assertEquals(rowDict2.get("return_rate"),
                          (-18 - 381 - 400.0 + 3816.0) / (12000 + 13508 + 13508 + 251220.0 + 45000),
                          "return isn't expected")

    def test_check_date_detail(self):
        # test trade_id=1987
        """
        stock_cust_return_by_prd.prd_ind	建筑机械与重型卡车
        stock_cust_return_by_prd.prd_no	2.000816
        stock_cust_return_by_prd.return	-115226.41999999993
        stock_cust_return_by_prd.return_rate	-0.009502134028067698
        stock_cust_return_by_prd.trade_id	1987
        stock_cust_return_by_prd.return_ratio	0.6923033340521149
        stock_cust_return_by_prd.return_rank	4
        stock_cust_return_by_prd.return_rate_rank	2
        stock_cust_return_by_prd.busi_date	2017-03-23
        stock_cust_return_by_prd.compute	7
        """
        r = Row(trade_id=u'1987', prd_no=u'2.000816',
                prd_ind=u'\u5efa\u7b51\u673a\u68b0\u4e0e\u91cd\u578b\u5361\u8f66',
                detail_list=[{u'return': u'-115226.41999999993', u'trd_type': u'long_related',
                              u'pos_cash_flow': u'6063186.42',
                              u'exception_label': u'0', u'pre_mkt_val': u'0.0',
                              u'now_mkt_val': u'5947960.0', u'busi_date': u'2017-03-23',
                              u'neg_cash_flow': u'6063186.42'}])
        rowDict2 = _travel_row(r, '2017-03-18').get("return")
        self.assertTrue(int(rowDict2) == -115226)
        # """
        # stock_cust_return_by_prd.prd_ind	基础化工
        # stock_cust_return_by_prd.prd_no	1.600301
        # stock_cust_return_by_prd.return	-5924.530000000028
        # stock_cust_return_by_prd.return_rate	-0.009933835241121989
        # stock_cust_return_by_prd.trade_id	12466
        # stock_cust_return_by_prd.return_ratio	0.051785613453405335
        # stock_cust_return_by_prd.return_rank	4
        # stock_cust_return_by_prd.return_rate_rank	6
        # stock_cust_return_by_prd.busi_date	2017-03-23
        # stock_cust_return_by_prd.compute	7
        # """
        r2 = Row(trade_id=u'12466', prd_no=u'1.600301', prd_ind=u'\u57fa\u7840\u5316\u5de5',
                 detail_list=[
                     {u'return': u'225.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
                      u'exception_label': u'0', u'pre_mkt_val': u'296100.0',
                      u'now_mkt_val': u'296325.0', u'busi_date': u'2017-03-22',
                      u'neg_cash_flow': u'0.0'},
                     {u'return': u'-4050.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
                      u'exception_label': u'0', u'pre_mkt_val': u'296325.0',
                      u'now_mkt_val': u'292275.0', u'busi_date': u'2017-03-23',
                      u'neg_cash_flow': u'0.0'},
                     {u'return': u'-2099.530000000028', u'trd_type': u'long_related',
                      u'pos_cash_flow': u'298199.53', u'exception_label': u'0',
                      u'pre_mkt_val': u'0.0', u'now_mkt_val': u'296100.0',
                      u'busi_date': u'2017-03-21', u'neg_cash_flow': u'298199.53'}])
        rd = _travel_row(r2, "2017-03-18").get("return")
        self.assertTrue(int(rd), -5924)
        rd_rate = _travel_row(r2, "2017-03-18").get("return_rate")
        print rd_rate

    def test_checkdata_sql(self):
        df = self.spark.sql("""
        select df.trade_id,df.prd_no,df.return,df.return_rate,df.return_ratio,df.return_rank,
        df.return_rate_rank,busi_date from adatatest.stock_cust_return_by_prd df
        where return>0
         """)

        df.where(df.trade_id == '17898').where(df.busi_date == '2017-03-31').where(
            df.prd_no == '2.002763').select(df.trade_id, df.prd_no, "return", df.return_rate,
                                            df.return_ratio, df.return_rank, df.return_rate_rank,
                                            df.busi_date).orderBy("return_rank", "busi_date").show()

        df.where(df.trade_id == '17898').where(df.busi_date == '2017-03-31').where(
            df.prd_no == '2.002763').select("return_ratio").orderBy("return_rank", "busi_date")
