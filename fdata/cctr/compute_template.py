# -*- coding: utf-8 -*-
from leek.common.util import get_trading_day, get_config, save_data
from pyspark import StorageLevel
from pyspark.sql import Row, functions as fn
from pyspark.sql.types import *


def filter_data(data, filter_func):
    a = dict()
    a["trade_id"] = data["trade_id"]
    a["secu_acc_id"] = data["secu_acc_id"]
    a["prd_no"] = data["prd_no"]
    a["busi_date"] = data["busi_date"]
    a["open_detail"] = filter(filter_func, data["open_detail"])
    return Row(**a)

class computeTemplate(object):
    def __init__(self, spark):
        self.spark = spark
        config = get_config()
        self.conf = config
        self.odata = config["original_database"]
        self.fdata = config["feature_database"]
        self.trading_calendar = config["calendar_table"]
        self.date_order, self.order_date = self.get_calendar()

    def get_calendar(self):
        """
        """
        return get_trading_day(self.spark, self.odata, self.trading_calendar)

    def is_trading_day(self, busi_date):
        """
        """
        return busi_date in self.date_order.value

    def save_trade_close_data(self, data, spark, busi_date):
        tempTable = "temp_cctr_long"
        data = data.filter(lambda x: len(x["close_detail"]) > 0)
        if data.count() > 0:
            data.toDF().createOrReplaceTempView(tempTable)
            sqlCmd = """
                select trade_id,secu_acc_id,prd_no,
                cd['open_date'] open_date,
                cd['close_type'] close_type,
                cd['close_date'] close_date,
                cast(cd['close_timestamp'] as bigint) close_timestamp,
                cd['busi_date'] busi_date,
                cast(cd['open_qty'] as bigint) open_qty,
                cast(cd['open_amt'] as double) open_amt,
                cast(cd['close_qty'] as bigint) close_qty,
                cast(cd['close_amt'] as double) close_amt,
                cast(cd['return'] as double) return,
                cast(cd['return_rate'] as double) return_rate,
                cast(cd['weighted_term'] as double) weighted_term,
                cast(cd['exception_label'] as bigint) exception_label
                from temp_cctr_long lateral view explode(close_detail) cd as cd
            """
            findDf = spark.sql(sqlCmd).withColumn('busi_date', fn.lit(busi_date)).persist(
                StorageLevel.DISK_ONLY)
            save_data(spark, self.fdata, self.close_table, busi_date, findDf)
            spark.catalog.dropTempView(tempTable)

    def save_trade_unclose_data(self, data, spark, busi_date, filter_func):
        data = data.map(lambda x: filter_data(x, filter_func))
        data = data.filter(lambda x: len(x["open_detail"]) > 0)
        if data.count() > 0:
            dataCal = data.toDF().persist(StorageLevel.DISK_ONLY).repartition(20)
            save_data(spark, self.fdata, self.unclose_cal, busi_date, dataCal)
            data = data.flatMap(self._trans_unclose)\
                .toDF().persist(StorageLevel.DISK_ONLY).repartition(20)
            data = data.select("trade_id", "secu_acc_id", "prd_no", "open_date", "open_timestamp",
                               "open_type", "busi_date", "orig_trd_qty", "orig_trd_amt", "trd_qty",
                               "trd_amt", "close_qty", "close_amt", "unclose_qty", "unclose_amt",
                               "return", "return_rate", "weighted_term", "exception_label")
            save_data(spark, self.fdata, self.unclose_table, busi_date, data)