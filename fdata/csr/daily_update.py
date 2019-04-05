# -*- coding: utf-8 -*-


from leek.common.util import save_data, setup_spark
from leek.fdata.csr.compute_template import computeTemplate
from leek.fdata.csr.long_close_return import longCloseReturn
from leek.fdata.csr.short_close_return import shortCloseReturn

from pyspark.sql import functions as func
from pyspark.sql import Row

import sys


class closeReturnUpdate(computeTemplate):
    """
    """

    def __init__(self, spark):
        computeTemplate.__init__(self, spark)

    def init_data(self, busi_date):
        """
        """
        # 如果不是交易日，则不进行计算
        if not self.is_trading_day(busi_date):
            return
        spark = self.spark
        long_close = longCloseReturn(spark)
        long_close.init_data(busi_date)
        short_close = shortCloseReturn(spark)
        short_close.init_data(busi_date)

    def update(self, busi_date):
        """
        """
        # 如果不是交易日，则不进行计算
        if not self.is_trading_day(busi_date):
            return
        spark = self.spark
        long_close = longCloseReturn(spark)
        long_close.daily_compute(busi_date)
        short_close = shortCloseReturn(spark)
        short_close.daily_compute(busi_date)


def _run(mode, load_date):
    """
    """
    spark = setup_spark({})
    daily_update = closeReturnUpdate(spark)
    if mode == "init":
        daily_update.init_data(load_date)
    else:
        daily_update.update(load_date)


if __name__ == "__main__":
    # 根据传入日期，模拟当天的交易数据，并更新持仓数据
    if len(sys.argv) == 3:
        _, mode, load_date = sys.argv
        _run(mode, load_date)
    else:
        pass
