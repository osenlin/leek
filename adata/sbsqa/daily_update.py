# -*- coding: utf-8 -*-
import sys
import datetime
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import setup_spark
from leek.adata.sbsqa.stock_ac_check_exception import StockAcCheckException
from leek.adata.sbsqa.stock_ac_check_exception_report import StockAcCheckExceptionReport
from leek.adata.sbsqa.stock_dr_check_exception_data import StockDrCheckExceptionData
from leek.adata.sbsqa.stock_dr_check_exception_report import StockDrCheckExceptionReport
from leek.adata.sbsqa.asset_dr_check_exception_data import AssetDrCheckExceptionData
from leek.adata.sbsqa.asset_dr_check_exception_report import AssetDrCheckExceptionReport
from leek.adata.sbsqa.stock_prd_ind_check_exception_data import StockPrdIndCheckExceptionData
from leek.adata.sbsqa.stock_prd_ind_check_exception_report import StockPrdIndCheckExceptionReport
from leek.adata.sbsqa.stock_prd_mon_check_exception_data import StockPrdMonCheckExceptionData
from leek.adata.sbsqa.stock_prd_mon_check_exception_report import StockPrdMonCheckExceptionReport

TASK = [
    ("StockAcCheckException", 1, u"1.记录持仓量核对出现异常和收益率核对出现异常的明细数据"),
    ("StockAcCheckExceptionReport", 1, u"2.记录持仓量核对出现异常和收益率核对出现异常的统计数据"),
    ("StockDrCheckExceptionData", 1, u"3.记录客户股票每日收益情况出现异常的明细数据"),
    ("StockDrCheckExceptionReport", 1, u"4.记录客户股票每日收益情况出现异常的统计数据"),
    ("AssetDrCheckExceptionData", 1, u"5.记录客户股票每日收益情况出现异常的明细数据"),
    ("AssetDrCheckExceptionReport", 1, u"6.记录客户股票每日收益情况出现异常的统计数据"),
    ("StockPrdIndCheckExceptionData", 1, u"7.记录股票以及所在办款的交叉对比的异常数据"),
    ("StockPrdIndCheckExceptionReport", 1, u"8.记录股票以及所在办款的交叉对比的统计数据"),
    ("StockPrdMonCheckExceptionData", 1, u"7.记录股票以及所在板块月份的交叉对比的异常数据"),
    ("StockPrdMonCheckExceptionReport", 1, u"8.记录股票以及所在板块月份的交叉对比的统计数据")
]


class DailyUpdate(LeekSparkJob):
    """
    """

    def __init__(self, spark, busi_date, mode):
        LeekSparkJob.__init__(self, spark)
        # T-1Date
        self.enddate = busi_date
        # 如果不是交易日，则不进行计算
        if not self.is_trading_day(self.enddate):
            print("task start error day[%s] is not trading day" % self.enddate)
            sys.exit(0)
        self.mode = mode

    def process(self):
        if self.mode == 'init':
            # raise Exception("this module has not init function")
            for (clazz, type, des) in TASK:
                print '-' * 50
                print u'task[{}],enddate[{}]'.format(clazz, self.enddate)
                eval(clazz)(self.sparkSession).init_data()
                print '-' * 50
        else:
            for (clazz, type, des) in TASK:
                print '-' * 50
                try:
                    if type == 3:
                        # for compute in [60]:
                        for compute in [7, 30, 60, 180]:
                            oldtime = datetime.datetime.now()
                            sdate = datetime.datetime.strptime(self.enddate, "%Y-%m-%d") \
                                + datetime.timedelta(days=-compute + 1)
                            startdate = sdate.strftime("%Y-%m-%d")
                            print u'>>task[{}],startdate[{}],enddate[{}] period[{}]'\
                                .format(clazz, startdate, self.enddate, compute)
                            eval(clazz)(self.sparkSession).daily_compute(startdate, self.enddate,
                                                                         compute)
                            newtime = datetime.datetime.now()
                            print u'>>task[{}] success! spend {} seconds'\
                                .format(clazz, (newtime - oldtime).seconds)
                    elif type == 2:
                        oldtime = datetime.datetime.now()
                        startdate = self.enddate[0:7] + '-01'
                        print u'>>task[{}],startdate[{}],enddate[{}]'.format(clazz, startdate,
                                                                             self.enddate)
                        eval(clazz)(self.sparkSession).daily_compute(startdate, self.enddate)
                        newtime = datetime.datetime.now()
                        print u'>>task[{}] success! spend {} seconds'\
                            .format(clazz, (newtime - oldtime).seconds)
                    else:
                        oldtime = datetime.datetime.now()
                        startdate = self.enddate
                        print u'>>task[{}],startdate[{}],enddate[{}]'.format(clazz, startdate,
                                                                             self.enddate)
                        eval(clazz)(self.sparkSession).daily_compute(startdate, self.enddate)
                        newtime = datetime.datetime.now()
                        print u'>>task[{}] success! spend {} seconds'\
                            .format(clazz, (newtime - oldtime).seconds)
                except Exception as e:
                    print e.message
                    raise e
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
