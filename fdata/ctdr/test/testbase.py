import pyspark
import unittest
import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class testBase(unittest.TestCase):
    logLevel = 'debug'

    def setUp(self):
        self.caclData = "2017-03-17"
        os.environ[
            'SPARK_HOME'] = \
            "/Users/linzhihuang/Downloads/spark-2.2.0-bin-hadoop2.7"
        sys.path.append(
            "/Users/linzhihuang/Downloads/spark-2.2.0-bin-hadoop2.7/python")
        conf = SparkConf()
        conf.setMaster("local").setAppName("hello").setSparkHome(
            "/Users/linzhihuang/Downloads/spark-2.2.0-bin-hadoop2.7")
        self.spark = SparkSession(SparkContext(conf=conf))

    @staticmethod
    def setupSpark(clusterConf, master, appName):

        if testBase.logLevel == 'debug':
            class Log:
                def sql(self, str):
                    """

                    :rtype: object
                    """
                    print(
                        "----------------command line start-------------------")
                    print("{0}{1}{2}".format("spark.sql(\"\"\"", str,
                                             "\"\"\").show()"))
                    print(
                        "----------------command line end---------------------")

            sparkSession = Log()
        else:
            conf = pyspark.SparkConf().setMaster(master).setAppName(appName)
            if not clusterConf:
                # todo iterate clusterConf
                pass
            else:
                # todo default config
                conf.set("spark.executor.memory", "1g")
            sparkSession = SparkSession.builder.config(
                conf=pyspark.SparkConf()).enableHiveSupport().getOrCreate()

        return sparkSession

    @staticmethod
    def ps():
        print 'helloword'


if __name__ == '__main__':
    testBase.unittest.main()
