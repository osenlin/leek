# -*- coding: utf-8 -*-
import sys
import datetime
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import setup_spark
from leek.adata.ssidqa.stock_cdc_check_exception_data import StockCDCCheckException
from leek.adata.ssidqa.stock_trdrr_check_exception_data import StockTrdrrCheckException
from leek.adata.ssidqa.stock_otbd_check_exception_data import StockOTBDCheckException
from leek.adata.ssidqa.stock_odc_check_exception_data import StockODCCheckException
from leek.adata.ssidqa.stock_oprd_check_exception_data import StockOprdCheckException
from leek.adata.ssidqa.stock_inv_pl_check_exception_data import StockInvPlCheckException

TASK = [
    ("StockTrdrrCheckException", 1, u"1.记录交易明细收益率出现异常的明细数据"),
    # ("StockCDCCheckException", 1, u"1.闭仓收益与每股每日收益对比"),
    ("StockOTBDCheckException", 1, u"开仓收益和每日交易收益交叉比对出现异常的明细数据"),
    ("StockODCCheckException", 1, u"开仓收益和每股每日收益交叉比对出现异常的明细数据"),
    ("StockOprdCheckException", 1, u"开仓收益和清仓股票收益交叉比对出现异常的明细数据"),
    ("StockInvPlCheckException", 1, u"开仓收益和每股每日收益交叉比对出现异常的明细数据")
]


class DailyUpdate(LeekSparkJob):

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
