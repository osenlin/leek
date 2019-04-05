# coding=utf-8
import datetime
from leek.common.util import get_trading_day, get_config


def func_logging(level):
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            print ">>>[{level}]: enter function {func}()".\
                format(level=level, func=func.__name__)
            oldtime = datetime.datetime.now()
            f = func(*args, **kwargs)
            newtime = datetime.datetime.now()
            print u'>>>[%s]:task_function[%s] spend %s  seconds' % (
                level, func.__name__, (newtime - oldtime).seconds)
            return f

        return inner_wrapper

    return wrapper


import ConfigParser


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
        self.confIni = ConfigParser.ConfigParser()
        if self.logLevel != 'debug':
            self.conf_path = spark.conf.get('spark.finogeeks.sbs.conf.path')
            self.confIni.read([self.conf_path + "/conf/config.ini"])
            self.sparkSession = spark
            self.trading_calendar = config["calendar_table"]
            self.date_order, self.order_date = self.get_calendar()
        else:
            class Log:
                def sql(self, str):
                    """
                    :rtype: object
                    """
                    print("-------------command line start-------------------")
                    print("{0}{1}{2}".format("spark.sql(\"\"\"", str,
                                             "\"\"\").show()"))
                    print("-------------command line end---------------------")
                    return self

                def repartition(self, partions):
                    print partions
                    return self

                def createOrReplaceTempView(self, str):
                    print str
                    return self

            self.sparkSession = Log()
            self.confIni.read(["../conf/config.ini"])

    def get_calendar(self):
        """
        """
        return get_trading_day(self.sparkSession, self.odata,
                               self.trading_calendar)

    def is_trading_day(self, busi_date):
        """
        """
        return busi_date in self.date_order.value

    def _alter_table_partiton(self, table, partition):
        self.sparkSession.sql("")

    def __call__(self, func):  # 接受函数
        def wrapper(*args, **kwargs):
            print "[{logLevel}]: enter function {func}()".format(
                level=self.logLevel, func=func.__name__)
            func(*args, **kwargs)

        return wrapper  # 返回函数
