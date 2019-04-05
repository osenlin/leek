# coding=utf-8
from testbase import testBase
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, Row
from leek.adata.ssid.stock_cust_investment_ability import StockCustInvestmentAbility, _travel_row2
from sklearn import linear_model
from pyspark.sql import functions as F
from datetime import datetime, timedelta


class SCDHTest(testBase):

    def setUp(self):
        StockCustInvestmentAbility.logLevel = 'debug'
        self.scdh = StockCustInvestmentAbility(None)
        os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        conf = SparkConf().setMaster("local").setAppName("hello")
        self.spark = SparkSession(SparkContext(conf=conf))

        self.enddate = "2018-03-18"

    def test_get_base_data(self):
        self.scdh._get_base_data("2017-03-16", "2017-03-16")

    def test_average(self):
        m = {"trade_id": "average", "return_rate": 0.0, "re_variance": 0.0, "up_capture": 1.0,
             "down_capture": 1.0, "alpha": 0.0, "beta": 1.0, "cl_value": 0.0}
        self.spark.createDataFrame([m]).rdd.count()

    def test_total_order(self):
        df = self.spark.createDataFrame(
            [{"b": 2, "c": 4, "a": 3}, {"a": 2, "c": 6, "b": 3}, {"a": 5, "c": 2, "b": 3},
             {"a": 1, "c": 3, "b": 3}])
        df.withColumn("hell", F.lit(8)).show()

        def f(row):
            t = row[0].asDict()
            t["id"] = row[1]
            return t

        indexrdd = df.select(df.a, df.b).rdd.sortBy(ascending=True, numPartitions=3,
                                                    keyfunc=lambda x: x.a).zipWithIndex().map(
            lambda x: f(x))
        print indexrdd.count()
        indexdf = self.spark.createDataFrame(indexrdd)
        indexdf.show()
        dp = self.spark.createDataFrame(
            df.rdd.sortBy(ascending=True, numPartitions=3, keyfunc=lambda x: x.c).zipWithIndex())
        dp.show()

    def test_numpy(self):
        import numpy as np
        X = np.mat([(3, 2, 1), (4, 1, 1)])
        Y = np.mat([[1], [2]])
        print 'X.T', X.T
        print 'X', X
        print 'Y', Y
        C = (X.T * X)
        print 'C=X.T*X', C
        print 'C.I', C.I
        print C.I * X.T
        # print 'C.I*X.T',C.I*np.asmatrix(X.T)
        S = C.I * X.T * Y
        print 'C.I*X.T*Y', S
        print S.item((0, 0))
        print S.item((1, 0))

    def test_get_natural_date(self):
        def get_natural_date(d1, diff_days, format="%Y-%m-%d"):
            if isinstance(d1, str):
                realdate = datetime.strptime(d1, format)
            elif isinstance(d1, datetime):
                realdate = datetime
            else:
                raise Exception("d1 isn't str or datetime")
            return realdate + timedelta(days=diff_days)

        print get_natural_date("2017-03-08", 1)

    def test_linear_model(self):
        reg = linear_model.LinearRegression()
        reg.fit([[-0.0248, 1], [-0.0403, 1]], [-0.02483243, -0.04525424])
        print reg.coef_

    def test_travel_row2(self):
        r = [Row(trade_id=u'10096',
                 detail_list=[{u'daily_return_rate': u'0.0052',
                               u'total_return': u'37296.909999999916',
                               u'exception_label': u'0',
                               u'total_return_rate': u'0.00516756633183234',
                               u'busi_date': u'2017-03-16'},
                              {u'daily_return_rate': u'-0.0103',
                               u'total_return': u'-110440.76000000001',
                               u'exception_label': u'0',
                               u'total_return_rate': u'-0.01525423955665218',
                               u'busi_date': u'2017-03-17'}]),
             Row(trade_id=u'10351',
                 detail_list=[{u'daily_return_rate': u'0.0052',
                               u'total_return': u'0.0',
                               u'exception_label': u'0',
                               u'total_return_rate': u'0.0',
                               u'busi_date': u'2017-03-16'},
                              {u'daily_return_rate': u'-0.0103',
                               u'total_return': u'0.0',
                               u'exception_label': u'0',
                               u'total_return_rate': u'0.0',
                               u'busi_date': u'2017-03-17'}]),
             Row(trade_id=u'10436',
                 detail_list=[{u'daily_return_rate': u'0.0052',
                               u'total_return': u'-3698.730000000098',
                               u'exception_label': u'0',
                               u'total_return_rate': u'-9.337162042763987E-4',
                               u'busi_date': u'2017-03-16'},
                              {u'daily_return_rate': u'-0.0103',
                               u'total_return': u'-36565.71999999991',
                               u'exception_label': u'0',
                               u'total_return_rate': u'-0.009239364328382658',
                               u'busi_date': u'2017-03-17'}])]
        reg = linear_model.LinearRegression(fit_intercept=False)
        import numpy as np
        for item in r:
            source = _travel_row2(item, self.enddate)
            rf = 0.03
            detailList = sorted(item.detail_list, key=lambda x: x['busi_date'])
            Y = np.mat([[float(prt["total_return_rate"]) - rf] for prt in detailList])
            betaX = np.mat([[float(mrt["daily_return_rate"]) - rf, 1] for mrt in detailList])
            reg.fit(betaX, Y)
            target = reg.coef_
            # 特殊处理情况下没问题
            print source.get("beta"), source.get("alpha")
            print target.item((0, 0)), target.item((0, 1))

    def test__travel_row2_0(self):
        s = Row(trade_id=u'11888', detail_list=[
            {u'daily_return_rate': u'0.0', u'total_return': u'0.0', u'exception_label': u'0',
                u'total_return_rate': u'0', u'busi_date': u'2017-03-16'},
            {u'daily_return_rate': u'0.4', u'total_return': u'0.0', u'exception_label': u'0',
                u'total_return_rate': u'0', u'busi_date': u'2017-03-17'}])
        print _travel_row2(s)

    def test_init_data(self):
        self.scdh.init_data()
