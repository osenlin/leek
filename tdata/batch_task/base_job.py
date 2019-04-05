# coding=utf-8

from leek.common.util import get_trading_day, get_config


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
        self.tdata = self.conf['tool_database']
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

    def batch_drop_partition(self, startdate, enddate, db, table_name):
        self.sparkSession.sql("use %s" % db)
        tables = [i.name for i in self.sparkSession.catalog.listTables()]
        if table_name in tables:
            sqlTradingDay = """
                select busi_date from {2}.{3}
                where busi_date>='{0}' and busi_date <='{1}'
            """.format(startdate, enddate, self.odata, self.trading_calendar)
            dfDay = self.sparkSession.sql(sqlTradingDay).collect()
            for day_item in dfDay:
                dropsql = "alter table %s.%s drop if exists partition(busi_date='%s')" % (
                    db, table_name, day_item.busi_date)
                self.sparkSession.sql(dropsql)
