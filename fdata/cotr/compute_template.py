# -*- coding: utf-8 -*-


from leek.common.util import get_trading_day, get_config, save_data
from leek.fdata.cotr.util import filter_data

from pyspark.sql import Row


class computeTemplate(object):
    """
    """

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
        return get_trading_day(
            self.spark, self.odata, self.trading_calendar)

    def is_trading_day(self, busi_date):
        """
        """
        return busi_date in self.date_order.value

    def save_close_data(self, data, busi_date, filter_func, sort_func):
        """
        """
        data = data.map(lambda x: filter_data(x, filter_func))
        data = data.filter(lambda x: len(x["open_detail"]) > 0)
        if data.count() > 0:
            data = data.flatMap(self._trans_close).toDF()
            data = sort_func(data)
            save_data(self.spark, self.fdata,
                      self.close_table, busi_date, data)
        else:
            pass

    def save_unclose_data(self, data, busi_date, filter_func, sort_func):
        """
        """
        data = data.map(lambda x: filter_data(x, filter_func))
        data = data.filter(lambda x: len(x["open_detail"]) > 0)
        if data.count() > 0:
            data = data.flatMap(self._trans_unclose).toDF()
            data = sort_func(data)
            save_data(self.spark, self.fdata,
                      self.unclose_table, busi_date, data)
        else:
            pass

    def save_unclose_cal(self, data, busi_date, filter_func, sort_func):
        """
        """
        data = data.map(lambda x: filter_data(x, filter_func))
        data = data.filter(lambda x: len(x["open_detail"]) > 0)
        data = data.map(lambda x: Row(**x))
        if data.count() > 0:
            data = data.toDF()
            data = sort_func(data)
            save_data(self.spark, self.fdata,
                      self.unclose_cal, busi_date, data)
        else:
            pass

