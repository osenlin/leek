# -*- coding: utf-8 -*-
import sys
import datetime
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import get_date, setup_spark
from leek.adata.sbs.stock_cust_daily_holding import StockCustDailyHolding
from leek.adata.sbs.stock_cust_daily_return import StockCustDailyReturn
from leek.adata.sbs.asset_cust_daily_return import AssetCustDailyReturn
from leek.adata.sbs.stock_return_by_month import StockReturnByMonth
from leek.adata.sbs.stock_cust_return_by_prd import StockCustReturnByPrd
from leek.adata.sbs.stock_cust_return_by_prd_ind import StockCustReturnByPrdInd

TASK = [
    ("StockCustDailyHolding", 1, u"1.客户每日在股票市场上的持仓和每支股票的收益情况"),
    ("StockCustDailyReturn", 1, u"2.记录客户每日在股票市场上做多、做空以及总的收益情况"),
    ("AssetCustDailyReturn", 1, u"3.记录客户资产的收益情况"),
    ("StockReturnByMonth", 2, u"4. 记录客户每个月的收益情况"),
    ("StockCustReturnByPrd", 3, u"5. 收益来源统计"),
    ("StockCustReturnByPrdInd", 3, u"6. 收益来源统计")
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
