# -*- coding: utf-8 -*-
import sys
from leek.common.util import get_config, setup_spark,get_trading_day
from leek.tdata.batch_task.base_job import LeekSparkJob
from leek.tdata.batch_task.stock_cash_flow_merge import StockCashFlowMerge
from leek.tdata.batch_task.stock_account_check import StockAccountCheck
from leek.tdata.batch_task.stock_account_check_exception_stat import StockAccountCheckExceptionStat
from leek.tdata.batch_task.stock_cust_daily_holding import StockCustDailyHolding
from leek.tdata.batch_task.stock_cust_daily_return import StockCustDailyReturn
from leek.tdata.batch_task.asset_cust_daily_return import AssetCustDailyReturn
TASK = [
    ("StockCashFlowMerge", u"1.现金流合并模块"),
    ("StockAccountCheck", u"2.账户核对基础数据"),
    ("StockAccountCheckExceptionStat", u"4.账户异常统计模块"),
    ("StockCustDailyHolding",u"每日持仓收益"),
    ("StockCustDailyReturn",u"每日持仓收益"),
    ("AssetCustDailyReturn",u"每日收益")
]


class DailyUpdate(LeekSparkJob):
    """
    """
    logLevel = 'info'

    def __init__(self, spark, start_date, end_date, mode):
        LeekSparkJob.__init__(self, spark)
        # T-1Date
        self.startdate = start_date
        self.enddate = end_date
        if not self.is_trading_day(self.startdate):
            print("task start error day[%s] is not trading day" % self.startdate)
            sys.exit(0)
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


def _run(mode, start_date, end_date):
    """
    """
    spark = setup_spark({})
    daily_update = DailyUpdate(spark, start_date, end_date, mode)
    daily_update.process()
    spark.stop()


if __name__ == "__main__":
    # 根据传入日期，模拟当天的交易数据，并更新持仓数据
    if len(sys.argv) == 4:
        print sys.argv
        _, mode, start_date, end_date = sys.argv
        _run(mode, start_date, end_date)
    else:
        print 'end'
        pass
