# coding=utf-8
from leek.adata.ssid.ssidjob import LeekSparkJob
from pyspark.sql import functions as F
from leek.common.util import save_data
from pyspark.storagelevel import StorageLevel


class StockCustPlAnalysis(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_pl_analysis = self.conf['stock_cust_pl_analysis_table']
        self.stock_close_trade_long_data = self.conf["long_close_trade_table"]
        self.stock_close_trade_short_data = self.conf["short_close_trade_table"]
        self.stock_unclose_trade_long_data = self.conf["long_unclose_trade_table"]
        self.stock_unclose_trade_short_data = self.conf["short_unclose_trade_table"]

    def init_data(self):
        sql = """
            create table if not exists {0}.{1}(
                trade_id string "客户代码",
                compute_term bigint "计算周期",
                total_gain double "总盈利",
                total_loss double "总亏损",
                total_return double "总收益",
                avg_return_rate double "平均收益率",
                avg_gain_rate double "平均盈利率",
                avg_loss_rate double "平均亏损率",
                trade_num bigint "交易次数",
                trade_gain_num bigint "盈利交易数",
                trade_loss_num bigint "亏损交易数",
                win_rate double "胜率",
                exception_label bigint "异常标签"
            )
            comment '记录客户盈亏情况'
            partitioned by (busi_date   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.stock_cust_pl_analysis)
        print sql

    def _get_close_data(self, startdate, enddate):
        sqlClose = """
            select trade_id,prd_no,busi_date,
            case when exception_label=1 then 0.0 else return end return,
            case when exception_label=1 then 0.0 else return_rate end return_rate,
            exception_label
            from {2}.{3}
            where busi_date>='{0}' and busi_date<='{1}'
        """
        sqlCloseLong = sqlClose.format(startdate, enddate, self.fdata,
                                       self.stock_close_trade_long_data)
        dfCloseLong = self.sparkSession.sql(sqlCloseLong)
        sqlCloseShort = sqlClose.format(startdate, enddate, self.fdata,
                                        self.stock_close_trade_short_data)
        dfCloseShort = self.sparkSession.sql(sqlCloseShort)
        df = dfCloseLong.union(dfCloseShort)
        df.persist(StorageLevel.DISK_ONLY).count()
        # df.show()
        return df

    def _get_unclose_data(self, startdate, enddate):
        sqlUnClose = """
            select trade_id,prd_no,busi_date,
            case when exception_label=1 then 0.0 else return end return,
            case when exception_label=1 then 0.0 else return_rate end return_rate,
            exception_label
            from {2}.{3}
            where busi_date>='{0}' and busi_date<='{1}'
        """
        # 未完结交易取最后一天
        sqlUnCloseLong = sqlUnClose.format(enddate, enddate, self.fdata,
                                           self.stock_unclose_trade_long_data)
        dfUnCloseLong = self.sparkSession.sql(sqlUnCloseLong)
        sqlUnCloseShort = sqlUnClose.format(enddate, enddate, self.fdata,
                                            self.stock_unclose_trade_short_data)
        dfUnCloseShort = self.sparkSession.sql(sqlUnCloseShort)
        df = dfUnCloseLong.union(dfUnCloseShort)
        df.persist(StorageLevel.DISK_ONLY).count()
        # df.show()
        return df

    def daily_compute(self, startdate=None, enddate=None, compute=7):
        """
        daily_compute
        :param startdate:
        :param enddate:
        :return:
        """
        dfCloseData = self._get_close_data(startdate, enddate)
        dfUnCloseData = self._get_unclose_data(startdate, enddate)
        dfBase = dfCloseData.union(dfUnCloseData)
        dfFinal = self._cal_result(dfBase, enddate, compute)
        self._save_result(dfFinal, compute, enddate)

    def _cal_result(self, df, enddate, compute):
        tempTable = "temp_stock_cust_pl_analysis"
        df.createOrReplaceTempView(tempTable)
        sql = """
            select *,cast(trade_gain_num*1.0/trade_num as double) win_rate
            from(
                select
                trade_id,
                '{1}' busi_date,
                {2} compute_term,
                sum(case when return>=0 then return else 0.0 end) total_gain,
                sum(case when return<0 then return else 0.0 end) total_loss,
                sum(return) total_return,
                avg(return_rate) avg_return_rate,
                nvl(avg(case when return>=0 then return_rate else null end),0.0) avg_gain_rate,
                nvl(avg(case when return<0 then return_rate else null end),0.0) avg_loss_rate,
                count(1) trade_num,
                sum(case when return>=0 then 1 else 0 end) trade_gain_num,
                sum(case when return<0 then 1 else 0 end) trade_loss_num,
                max(exception_label) exception_label
                from {0}
                group by  trade_id
            ) a
        """.format(tempTable, enddate, compute)
        tempDf = self.sparkSession.sql(sql)
        tempDf.persist(StorageLevel.DISK_ONLY).count()
        # tempDf.show()
        return tempDf

    def _save_result(self, df, compute, enddate):
        dropPartitionSql = """alter table {0}.{1} drop
                              if exists  partition(busi_date= '{2}',compute_term='{3}')"""\
            .format(self.adata, self.stock_cust_pl_analysis, enddate, compute)
        save_data(self.sparkSession, self.adata, self.stock_cust_pl_analysis, enddate,
                  df.repartition(30), partitonByName=["busi_date", "compute_term"],
                  dropPartitonSql=dropPartitionSql)
