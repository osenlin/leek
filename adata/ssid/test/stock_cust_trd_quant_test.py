# coding=utf-8
from testbase import testBase
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, Row
from leek.adata.ssid.stock_cust_trd_quant import StockCustTrdQuant


class SCDHTest(testBase):

    def setUp(self):
        StockCustTrdQuant.logLevel = 'debug'
        self.scdh = StockCustTrdQuant(None)
        os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        conf = SparkConf().setMaster("local").setAppName("hello")
        self.spark = SparkSession(SparkContext(conf=conf))

    def test_get_base_data(self):
        self.scdh._get_base_info("2017-05-31", "2017-05-31")

    def test_get_market_value(self):
        self.scdh._get_market_value("2017-03-16", "2017-03-16")

    def test__get_industry_prd_no(self):
        self.scdh._get_industry_prd_no("2017-03-16", "2017-03-16")

    def test_maxmin(self):
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
            from adatatest.stock_cust_trd_quant
            where busi_date='2017-03-16'
        """

        sql2 = """
            select *
            from stock_cust_trd_quant
            where high2trdprice=1
        """
        """
        stock_cust_trd_quant.trade_id	22127
        stock_cust_trd_quant.prd_no	1.601677
        stock_cust_trd_quant.trade_type	1
        stock_cust_trd_quant.timestamp	1490022002
        stock_cust_trd_quant.trdprice2low	-1
        stock_cust_trd_quant.high2trdprice	1
        stock_cust_trd_quant.stock_chg_3d	-0.0139
        stock_cust_trd_quant.stock_chg_5d	-0.0097
        stock_cust_trd_quant.stock_chg_10d	-0.0076
        stock_cust_trd_quant.stock_rank_perc_3d	0.6381285805219605
        stock_cust_trd_quant.stock_rank_perc_5d	0.5706556333545513
        stock_cust_trd_quant.stock_rank_perc_10d	0.4191597708465945
        stock_cust_trd_quant.market_chg_3d	0.0036
        stock_cust_trd_quant.market_chg_5d	0.0082
        stock_cust_trd_quant.market_chg_10d	0.0157
        stock_cust_trd_quant.industry_chg_3d	-0.0074
        stock_cust_trd_quant.industry_chg_5d	-0.0098
        stock_cust_trd_quant.industry_chg_10d	0.006
        stock_cust_trd_quant.ind_rank_perc_3d	0.8174273858921162
        stock_cust_trd_quant.ind_rank_perc_5d	0.6473029045643154
        stock_cust_trd_quant.ind_rank_perc_10d	0.3153526970954357
        stock_cust_trd_quant.busi_date	2017-03-20
        """

        sql3 = """
        select * from odata.stock_cash_flow_detail
        where trade_id='22127' and prd_no='1.601677' and busi_date='2017-03-20'
        """
