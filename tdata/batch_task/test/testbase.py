import pyspark
from pyspark.sql import SparkSession
import unittest


class testBase(unittest.TestCase):
    logLevel = 'debug'

    def setUp(self):
        self.caclData = "2017-03-17"
        self.sparkSession = testBase.setupSpark(None, "yarn", "app_cash_flow_merge")

    @staticmethod
    def setupSpark(clusterConf, master, appName):

        if testBase.logLevel == 'debug':
            class Log:
                def sql(self, str):
                    """

                    :rtype: object
                    """
                    print("------------------command line start-------------------")
                    print("{0}{1}{2}".format("spark.sql(\"\"\"", str, "\"\"\")"))
                    print("------------------command line end---------------------")

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
