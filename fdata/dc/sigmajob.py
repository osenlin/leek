# coding=utf-8
import pyspark
from pyspark.sql import SparkSession
from leek.common.util import get_trading_day, get_config, get_date


class LeekSparkJob:
    """
    python spark的作业基类
    """
    logLevel = 'info'

    def __init__(self, spark):

        config = get_config()
        self.conf = config
        self.odata = config["original_database"]
        self.fdata = config["feature_database"]
        self.adata = config["aggregation_database"]
        if self.logLevel != 'debug':
            self.sparkSession = spark
            self.trading_calendar = config["calendar_table"]
            self.date_order, self.order_date = self.get_calendar()
        else:
            class Log:
                def sql(self, str):
                    """
                    :rtype: object
                    """
                    print("------------------command line start-------------------")
                    print("{0}{1}{2}".format("spark.sql(\"\"\"", str, "\"\"\")"))
                    print("------------------command line end---------------------")
                    return self

                def repartition(self, partions):
                    return self

                def createOrReplaceTempView(self, str):
                    return self

            self.sparkSession = Log()

    def get_calendar(self):
        """
        """
        return get_trading_day(self.sparkSession, self.odata, self.trading_calendar)

    def is_trading_day(self, busi_date):
        """
        """
        return busi_date in self.date_order.value
