# -*- coding: utf-8 -*-


from leek.common.util import get_trading_day, get_config


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
