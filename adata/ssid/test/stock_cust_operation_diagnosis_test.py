# coding=utf-8
from testbase import testBase
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, Row
from leek.adata.ssid.stock_cust_diagnosis import StockCustOperationDiagnosis


class SCDHTest(testBase):

    def setUp(self):
        StockCustOperationDiagnosis.logLevel = 'debug'
        self.scdh = StockCustOperationDiagnosis(None)
        os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        conf = SparkConf().setMaster("local").setAppName("hello")
        self.spark = SparkSession(SparkContext(conf=conf))

    def test_get_base_data(self):
        self.scdh._get_base_sql("2017-03-16", "2017-03-16")

    def test_cond_1(self):
        sql = """
        select
            max(trdprice2low) max_trdprice2low,
            max(high2trdprice) max_high2trdprice,
            max(stock_chg_3d) max_stock_chg_3d,
            max(stock_chg_5d) max_stock_chg_5d,
            max(stock_chg_10d) max_stock_chg_10d,
            max(stock_rank_perc_3d) max_stock_rank_perc_3d,
            max(stock_rank_perc_5d) max_stock_rank_perc_5d,
            max(stock_rank_perc_10d) max_stock_rank_perc_10d,
            max(market_chg_3d) max_market_chg_3d,
            max(market_chg_5d) max_market_chg_5d,
            max(market_chg_10d) max_market_chg_10d,
            max(industry_chg_3d) max_industry_ind_3d,
            max(industry_chg_5d) max_industry_chg_5d,
            max(industry_chg_10d) max_industry_chg_10d,
            max(ind_rank_perc_3d) max_ind_rank_perc_3d,
            max(ind_rank_perc_5d) max_ind_rank_perc_5d,
            max(ind_rank_perc_10d) max_ind_rank_perc_10d,
            min(trdprice2low) min_trdprice2low,
            min(high2trdprice) min_high2trdprice,
            min(stock_chg_3d) min_stock_chg_3d,
            min(stock_chg_5d) min_stock_chg_5d,
            min(stock_chg_10d) min_stock_chg_10d,
            min(stock_rank_perc_3d) min_stock_rank_perc_3d,
            min(stock_rank_perc_5d) min_stock_rank_perc_5d,
            min(stock_rank_perc_10d) min_stock_rank_perc_10d,
            min(market_chg_3d) min_market_chg_3d,
            min(market_chg_5d) min_market_chg_5d,
            min(market_chg_10d) min_market_chg_10d,
            min(industry_chg_3d) min_industry_ind_3d,
            min(industry_chg_5d) min_industry_chg_5d,
            min(industry_chg_10d) min_industry_chg_10d,
            min(ind_rank_perc_3d) min_ind_rank_perc_3d,
            min(ind_rank_perc_5d) min_ind_rank_perc_5d,
            min(ind_rank_perc_10d) min_ind_rank_perc_10d
            from stock_cust_trd_quant
        """
