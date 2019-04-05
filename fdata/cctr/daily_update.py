# -*- coding: utf-8 -*-


from leek.common.util import setup_spark
import datetime
import sys
from leek.fdata.cctr.compute_template import computeTemplate
from leek.fdata.cctr.long_close_return import longCloseTradeReturn
from leek.fdata.cctr.short_close_return import shortCloseTradeReturn


class closeTradeReturnUpdate(computeTemplate):
    """
    """

    def __init__(self, spark):
        computeTemplate.__init__(self, spark)

    def init_data(self, busi_date):
        """
        """
        if not self.is_trading_day(busi_date):
            return
        spark = self.spark
        long_close_trade = longCloseTradeReturn(spark)
        long_close_trade.init_data(busi_date)
        short_close_trade = shortCloseTradeReturn(spark)
        short_close_trade.init_data(busi_date)

    def update(self, busi_date):
        """
        """
        if not self.is_trading_day(busi_date):
            return
        spark = self.spark
        long_close_trade = longCloseTradeReturn(spark)
        long_close_trade.daily_compute(busi_date)
        short_close_trade = shortCloseTradeReturn(spark)
        short_close_trade.daily_compute(busi_date)


def _run(mode, load_date):
    """
    """
    print '-' * 50
    oldtime = datetime.datetime.now()
    print 'prepare start'
    spark = setup_spark({})
    daily_update = closeTradeReturnUpdate(spark)
    newtime = datetime.datetime.now()
    print 'prepare end spend {} seconds'.format((newtime - oldtime).seconds)

    print '>>task cctr, startdate[%s]' % load_date
    oldtime = datetime.datetime.now()
    if mode == "init":
        daily_update.init_data(load_date)
    else:
        daily_update.update(load_date)
    newtime = datetime.datetime.now()
    print u'>>task success! spend {} seconds'.format(
        (newtime - oldtime).seconds)
    print '-' * 50


if __name__ == "__main__":
    # 根据传入日期，模拟当天的交易数据，并更新持仓数据
    if len(sys.argv) == 3:
        _, mode, load_date = sys.argv
        _run(mode, load_date)
    else:
        pass
