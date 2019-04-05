from testbase import testBase
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from leek.adata.sbs.stock_cust_return_by_prd_ind import _travel_row
from leek.adata.sbs.stock_cust_return_by_prd_ind import StockCustReturnByPrdInd
import os
import sys


class SCDHTest(testBase):

    def setUp(self):
        StockCustReturnByPrdInd.logLevel = 'debug'
        self.scdh = StockCustReturnByPrdInd(None)
        os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        conf = SparkConf().setMaster("local").setAppName("hello")
        self.spark = SparkSession(SparkContext(conf=conf))

    def tearDown(self):
        self.spark.stop()

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

    def test_travel_row(self):
        # """
        # stock_cust_return_by_prd_ind.prd_ind	unknown
        # stock_cust_return_by_prd_ind.return	-44623.789999999964
        # stock_cust_return_by_prd_ind.return_rate	-0.006018969744297111
        # stock_cust_return_by_prd_ind.trade_id	12466
        # stock_cust_return_by_prd_ind.return_ratio	0.4610100448676952
        # stock_cust_return_by_prd_ind.return_rank	2
        # stock_cust_return_by_prd_ind.return_rate_rank	1
        # stock_cust_return_by_prd_ind.busi_date	2017-03-23
        # stock_cust_return_by_prd_ind.compute	7
        # """
        # spark.sql("""
        #   select  trade_id,prd_ind,collect_list(detail_item) detail_list from (
        #     select trade_id,trim(prd_ind) prd_ind,
        #            (str_to_map(concat(
        #                 'pre_mkt_val:',pre_mkt_val,
        #                 ',now_mkt_val:',now_mkt_val,
        #                 ',pos_cash_flow:',pos_cash_flow,
        #                 ',neg_cash_flow:',pos_cash_flow,
        #                 ',exception_label:',exception_label,
        #                 ',trd_type:',trd_type,
        #                 ',return:',return,
        #                 ',busi_date:',busi_date),",",":")) detail_item
        #     from adatatest.stock_cust_daily_holding
        #     where  busi_date<='2017-03-23' and trade_id='12466' and prd_ind='unknown'
        #   ) a
        #   GROUP  by trade_id,prd_ind
        # """)
        r = Row(trade_id=u'12466', prd_ind=u'unknown', detail_list=[
            {u'return': u'-13008.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'1263402.0', u'now_mkt_val': u'1250394.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'6344.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'135176.0', u'now_mkt_val': u'141520.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'-12803.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'1308384.0', u'now_mkt_val': u'1295581.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'-4.229999999999563', u'trd_type': u'long_related',
             u'pos_cash_flow': u'16940.23', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'16936.0', u'busi_date': u'2017-03-23',
             u'neg_cash_flow': u'16940.23'},
            {u'return': u'1612.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'208052.0', u'now_mkt_val': u'209664.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'0.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'35466.53', u'now_mkt_val': u'18526.3',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'4730.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'679400.0', u'now_mkt_val': u'684130.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'-1662.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'271183.0', u'now_mkt_val': u'269521.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'-693.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-130207.0', u'now_mkt_val': u'-130900.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'-21138.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'1284540.0', u'now_mkt_val': u'1263402.0',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'2079.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-132286.0', u'now_mkt_val': u'-130207.0',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'6771.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'128405.0', u'now_mkt_val': u'135176.0',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'0.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'306163.19', u'now_mkt_val': u'35466.53',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'-12470.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'691870.0', u'now_mkt_val': u'679400.0',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'-122.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'128527.0', u'now_mkt_val': u'128405.0',
             u'busi_date': u'2017-03-21', u'neg_cash_flow': u'0.0'},
            {u'return': u'11.429999999999836', u'trd_type': u'long_related',
             u'pos_cash_flow': u'2273.57', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'2285.0', u'busi_date': u'2017-03-21', u'neg_cash_flow': u'2273.57'},
            {u'return': u'539.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-132825.0', u'now_mkt_val': u'-132286.0',
             u'busi_date': u'2017-03-21', u'neg_cash_flow': u'0.0'},
            {u'return': u'8673.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'1327382.0', u'now_mkt_val': u'1336055.0',
             u'busi_date': u'2017-03-21', u'neg_cash_flow': u'0.0'},
            {u'return': u'15399.439999999944', u'trd_type': u'long_related',
             u'pos_cash_flow': u'1274560.56', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'1289960.0', u'busi_date': u'2017-03-20',
             u'neg_cash_flow': u'1274560.56'},
            {u'return': u'197.7399999999907', u'trd_type': u'short_related',
             u'pos_cash_flow': u'0.0', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'-132825.0', u'busi_date': u'2017-03-20', u'neg_cash_flow': u'0.0'},
            {u'return': u'0.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'3497135.97', u'now_mkt_val': u'1510820.28',
             u'busi_date': u'2017-03-20', u'neg_cash_flow': u'0.0'},
            {u'return': u'12845.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-1333311.0', u'now_mkt_val': u'-1320466.0',
             u'busi_date': u'2017-03-17', u'neg_cash_flow': u'0.0'},
            {u'return': u'0.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'3497135.97', u'now_mkt_val': u'3497135.97',
             u'busi_date': u'2017-03-17', u'neg_cash_flow': u'0.0'},
            {u'return': u'0.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'2177000.0', u'now_mkt_val': u'3497135.97',
             u'busi_date': u'2017-03-16', u'neg_cash_flow': u'0.0'},
            {u'return': u'-17.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'2247.0', u'now_mkt_val': u'2230.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'15414.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-1333311.0', u'now_mkt_val': u'-1317897.0',
             u'busi_date': u'2017-03-23', u'neg_cash_flow': u'0.0'},
            {u'return': u'-38.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'2285.0', u'now_mkt_val': u'2247.0',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'5138.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-1338449.0', u'now_mkt_val': u'-1333311.0',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'-27671.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'1336055.0', u'now_mkt_val': u'1308384.0',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'-2808.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'210860.0', u'now_mkt_val': u'208052.0',
             u'busi_date': u'2017-03-22', u'neg_cash_flow': u'0.0'},
            {u'return': u'486.3400000000256', u'trd_type': u'long_related',
             u'pos_cash_flow': u'270696.66', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'271183.0', u'busi_date': u'2017-03-22',
             u'neg_cash_flow': u'270696.66'},
            {u'return': u'-2753.609999999986', u'trd_type': u'long_related',
             u'pos_cash_flow': u'694623.61', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'691870.0', u'busi_date': u'2017-03-21',
             u'neg_cash_flow': u'694623.61'},
            {u'return': u'-5420.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'1289960.0', u'now_mkt_val': u'1284540.0',
             u'busi_date': u'2017-03-21', u'neg_cash_flow': u'0.0'},
            {u'return': u'-2569.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-1335880.0', u'now_mkt_val': u'-1338449.0',
             u'busi_date': u'2017-03-21', u'neg_cash_flow': u'0.0'},
            {u'return': u'0.0', u'trd_type': u'long_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'1510820.28', u'now_mkt_val': u'306163.19',
             u'busi_date': u'2017-03-21', u'neg_cash_flow': u'0.0'},
            {u'return': u'1299.6199999999953', u'trd_type': u'long_related',
             u'pos_cash_flow': u'209560.38', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'210860.0', u'busi_date': u'2017-03-21',
             u'neg_cash_flow': u'209560.38'},
            {u'return': u'-15414.0', u'trd_type': u'short_related', u'pos_cash_flow': u'0.0',
             u'exception_label': u'0', u'pre_mkt_val': u'-1320466.0', u'now_mkt_val': u'-1335880.0',
             u'busi_date': u'2017-03-20', u'neg_cash_flow': u'0.0'},
            {u'return': u'5451.600000000093', u'trd_type': u'long_related',
             u'pos_cash_flow': u'1321930.4', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'1327382.0', u'busi_date': u'2017-03-20',
             u'neg_cash_flow': u'1321930.4'},
            {u'return': u'150.9100000000035', u'trd_type': u'long_related',
             u'pos_cash_flow': u'128376.09', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'128527.0', u'busi_date': u'2017-03-20',
             u'neg_cash_flow': u'128376.09'},
            {u'return': u'-13175.030000000028', u'trd_type': u'short_related',
             u'pos_cash_flow': u'0.0', u'exception_label': u'0', u'pre_mkt_val': u'0.0',
             u'now_mkt_val': u'-1333311.0', u'busi_date': u'2017-03-16', u'neg_cash_flow': u'0.0'}])
        r2 = _travel_row(r, '2017-03-23')
        self.assertTrue(int(r2.get("return")), -44623)
