# -*- encoding: UTF-8 -*-
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

from testbase import testBase
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import *
import os
import sys


class SCDHTest(testBase):

    def setUp(self):
        os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        conf = SparkConf().setMaster("local").setAppName("hello")
        self.spark = SparkSession(SparkContext(conf=conf))

    def tearDown(self):
        self.spark.stop()

    def test_mapPartition(self):
        df = self.spark.createDataFrame([{u"a": None, u"b": "2"}, {u"a": "3", u"b": "4"}])
        df2 = self.spark.createDataFrame([{u"a": "1", u"c": "2"}, {u"a": "4", u"c": "4"}])
        df.join(df2, "a", "left_outer").select(df["*"], F.when(F.isnull(df.a), F.lit("a null")),
                                               F.when((df.a == 3) & (df.b == 4), "gg").otherwise(
                                                   "dd")).show()
