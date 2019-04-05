# -*- coding: utf-8 -*-
import sys
import datetime
from leek.adata.ssid.ssidjob import LeekSparkJob
from leek.common.util import setup_spark, get_natural_date
from leek.adata.ssid.stock_cust_investment_ability import StockCustInvestmentAbility
from leek.adata.ssid.stock_cust_trd_quant import StockCustTrdQuant
from leek.adata.ssid.stock_cust_diagnosis_rank_buy import StockCustDiagnosisRankBuy
from leek.adata.ssid.stock_cust_diagnosis_rank_sell import StockCustDiagnosisRankSell
from leek.adata.ssid.stock_cust_investment_rank_score import StockCustInvestRankScore
from leek.adata.ssid.stock_cust_daily_return import StockCustDailyReturn
from leek.adata.ssid.stock_cust_pl_analysis import StockCustPlAnalysis
from leek.adata.ssid.stock_tradebyday_data import StockTradeByDayData
from leek.adata.ssid.stock_cust_daily_holding import StockCustDailyHolding

TASK = [
    ("StockCustTrdQuant", 1, u"1. 这个表将记录客户每次买卖后股票的涨跌幅及排名"),
    ("StockCustDailyHolding", 1, u"每日持仓收益"),
    # 需要StockCustDailyReturn任务会比sbs相同任务多一个字段part
    ("StockCustDailyReturn", 1, u"2.记录客户每日在股票市场上做多、做空以及总的收益情况"),
    ("StockTradeByDayData", 1, u"这个表每日完结的交易收益明细"),
    ("StockCustInvestmentAbility", 3, u"1. 记录客户在给定周期内投资行为的量化指标"),
    ("StockCustInvestRankScore", 3, u"2.记录客户在给定周期内投资行为量化指标的排名、得分"),
    # type == 4表示重启sparkSession
    ("", 4, ""),
    ("StockCustPlAnalysis", 3, u"3.4.2 记录客户盈亏情况"),
    ("StockCustDiagnosisRankBuy", 3, u"这个表将记录客户在给定周期内买入操作诊断指标"),
    ("StockCustDiagnosisRankSell", 3, u"这个表将记录客户在给定周期内卖出操作诊断指标")
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
            for (clazz, type, desc) in TASK:
                print '-' * 50
                print u'task[{}],enddate[{}]'.format(clazz, self.enddate)
                eval(clazz)(self.sparkSession).init_data()
                print '-' * 50
        else:
            for (clazz, type, desc) in TASK:
                # 对于类型4的任务，重新启动sparkSession
                if type == 4:
                    self.sparkSession.stop()
                    self.sparkSession = setup_spark({})
                else:
                    pass
                try:
                    if type == 3:
                        for compute in [90, 180, 360]:
                            oldtime = datetime.datetime.now()
                            startdate = get_natural_date(self.enddate, -compute + 1).strftime(
                                "%Y-%m-%d")
                            print u'>>task[{}],startdate[{}],enddate[{}] period[{}]'.\
                                format(clazz, startdate, self.enddate, compute)
                            eval(clazz)(self.sparkSession).daily_compute(startdate, self.enddate,
                                                                         compute)
                            newtime = datetime.datetime.now()
                            print u'>>task[{}] success! spend {} seconds'.\
                                format(clazz, (newtime - oldtime).seconds)
                    elif type == 2:
                        oldtime = datetime.datetime.now()
                        startdate = self.enddate[0:7] + '-01'
                        print u'>>task[{}],startdate[{}],enddate[{}]'.format(clazz, startdate,
                                                                             self.enddate)
                        eval(clazz)(self.sparkSession).daily_compute(startdate, self.enddate)
                        newtime = datetime.datetime.now()
                        print u'>>task[{}] success! spend {} seconds'.\
                            format(clazz, (newtime - oldtime).seconds)
                    elif type == 1:
                        oldtime = datetime.datetime.now()
                        startdate = self.enddate
                        print u'>>task[{}],startdate[{}],enddate[{}]'.format(clazz, startdate,
                                                                             self.enddate)
                        eval(clazz)(self.sparkSession).daily_compute(startdate, self.enddate)
                        newtime = datetime.datetime.now()
                        print u'>>task[{}] success! spend {} seconds'.\
                            format(clazz, (newtime - oldtime).seconds)
                except Exception as e:
                    print e.message
                    raise e


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
