# -*- coding: utf-8 -*-
from leek.fdata.dc.leekjob import LeekSparkJob
from leek.common.util import get_trading_day, get_config, get_date, setup_spark
from leek.fdata.dc.stock_cash_flow_merge import StockCashFlowMerge
from leek.fdata.dc.stock_account_check import StockAccountCheck
from leek.fdata.dc.stock_account_check_exception_detail import StockAccountCheckExceptionDetail
from leek.fdata.dc.stock_account_check_exception_stat import StockAccountCheckExceptionStat
import sys

TASK = [
    ("StockCashFlowMerge", u"1.现金流合并模块"),
    ("StockAccountCheck", u"2.账户核对基础数据"),
    ("StockAccountCheckExceptionDetail", u"3.账户异常明细数据"),
    ("StockAccountCheckExceptionStat", u"4.账户异常统计模块")
]


class DailyUpdate(LeekSparkJob):
    """
    """

    def __init__(self, spark, busi_date, mode):
        LeekSparkJob.__init__(self, spark)
        # T-1Date
        self.enddate = busi_date
        if not self.is_trading_day(self.enddate):
            print("task start error day[%s] is not trading day" % self.enddate)
            sys.exit(0)
        # T-2Date
        if self.logLevel != 'debug':
            self.startdate = get_date(self.date_order, self.order_date, self.enddate, -1)
        else:
            self.startdate = self.enddate

        self.mode = mode  # 如果不是交易日，则不进行计算

    def process(self):
        if self.mode == 'init':
            # raise Exception("this module has not init function")
            for (clazz, des) in TASK:
                print '-' * 50
                print u'task[{}],startdate[{}],enddate[{}]'.format(clazz, self.startdate,
                                                                   self.enddate)
                eval(clazz)(self.sparkSession).init_data()
                print '-' * 50
        else:
            for (clazz, des) in TASK:
                print '-' * 50
                print u'task[{}],startdate[{}],enddate[{}]'.format(clazz, self.startdate,
                                                                   self.enddate)
                eval(clazz)(self.sparkSession).daily_compute(self.startdate, self.enddate)
                print '-' * 50


def _run(mode, load_date):
    """
    """
    spark = setup_spark({})
    daily_update = DailyUpdate(spark, load_date, mode)
    daily_update.process()
    spark.stop()


if __name__ == "__main__":
    # 根据传入日期，模拟当天的交易数据，并更新持仓数据
    if len(sys.argv) == 3:
        _, mode, load_date = sys.argv
        _run(mode, load_date)
    else:
        pass
