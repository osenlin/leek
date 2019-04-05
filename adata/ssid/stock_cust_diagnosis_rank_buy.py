# coding=utf-8
from leek.adata.ssid.ssidjob import LeekSparkJob
from leek.common.util import save_data
from pyspark.sql import Window, functions as fn
from pyspark.storagelevel import StorageLevel


class StockCustDiagnosisRankBuy(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_trd_quant = self.conf["stock_cust_trd_quant_table"]
        self.stock_cust_diagnosis_sell_time = self.conf["stock_cust_diagnosis_sell_time_table"]
        self.stock_cust_diagnosis_sell_ind = self.conf["stock_cust_diagnosis_sell_ind_table"]
        self.stock_cust_diagnosis_sell_prd = self.conf["stock_cust_diagnosis_sell_prd_table"]
        self.stock_cust_diagnosis_buy_time = self.conf["stock_cust_diagnosis_buy_time_table"]
        self.stock_cust_diagnosis_buy_ind = self.conf["stock_cust_diagnosis_buy_ind_table"]
        self.stock_cust_diagnosis_buy_prd = self.conf["stock_cust_diagnosis_buy_prd_table"]

    def init_data(self):
        print 'init stock_cust_operation_diagnosis_sell_rank'
        print 'init stock_cust_operation_diagnosis_buy_rank'

    def _cal_percent_rank(self, dfBase, col_name, order):
        print "sort column[{0}],order by[{1}]".format(col_name, order)
        if order == "asc":
            window = Window.orderBy(col_name)
        else:
            window = Window.orderBy(fn.desc(col_name))
        df = dfBase.where("%s is not null" % col_name) \
            .withColumn(col_name + "_rank", fn.percent_rank().over(window)) \
            .persist(storageLevel=StorageLevel.DISK_ONLY) \
            .union(dfBase.where("%s is null" % col_name)
                   .withColumn(col_name + "_rank", fn.lit(None))) \
            .repartition(20).persist(storageLevel=StorageLevel.DISK_ONLY)
        df.count()
        return df

    def _cal(self, df, lo_col, hi_col, order1="asc", order2="desc"):
        dfTime = df.select("trade_id", "compute_term", "busi_date", lo_col, hi_col,
                           "avg_chg_market_3d", "avg_chg_market_5d", "avg_chg_market_10d")

        dfTime = self._cal_percent_rank(dfTime, lo_col, order1)
        dfTime = self._cal_percent_rank(dfTime, hi_col, order2)
        dfTime = self._cal_percent_rank(dfTime, "avg_chg_market_3d", order2)
        dfTime = self._cal_percent_rank(dfTime, "avg_chg_market_5d", order2)
        dfTime = self._cal_percent_rank(dfTime, "avg_chg_market_10d", order2)
        dfInd = df.select("trade_id", "compute_term", "busi_date",
                          "avg_chg_industry_3d", "avg_chg_industry_5d", "avg_chg_industry_10d",
                          "top_ind_perc_3d", "top_ind_perc_5d", "top_ind_perc_10d")
        dfInd = self._cal_percent_rank(dfInd, "avg_chg_industry_3d", order2)
        dfInd = self._cal_percent_rank(dfInd, "avg_chg_industry_5d", order2)
        dfInd = self._cal_percent_rank(dfInd, "avg_chg_industry_10d", order2)
        dfInd = self._cal_percent_rank(dfInd, "top_ind_perc_3d", order2)
        dfInd = self._cal_percent_rank(dfInd, "top_ind_perc_5d", order2)
        dfInd = self._cal_percent_rank(dfInd, "top_ind_perc_10d", order2)
        dfPrd = df.select("trade_id", "compute_term", "busi_date", "avg_chg_stock_3d",
                          "avg_chg_stock_5d", "avg_chg_stock_10d", "top_stock_perc_3d",
                          "top_stock_perc_5d", "top_stock_perc_10d")
        dfPrd = self._cal_percent_rank(dfPrd, "avg_chg_stock_3d", order2)
        dfPrd = self._cal_percent_rank(dfPrd, "avg_chg_stock_5d", order2)
        dfPrd = self._cal_percent_rank(dfPrd, "avg_chg_stock_10d", order2)
        dfPrd = self._cal_percent_rank(dfPrd, "top_stock_perc_3d", order2)
        dfPrd = self._cal_percent_rank(dfPrd, "top_stock_perc_5d", order2)
        dfPrd = self._cal_percent_rank(dfPrd, "top_stock_perc_10d", order2)
        return dfTime, dfInd, dfPrd

    def _get_base_data(self, startdate, enddate, compute=90):
        sql = """
            select
            avg(trdprice2low) {3},
            avg(high2trdprice) {4},
            avg(market_chg_3d) avg_chg_market_3d,
            avg(market_chg_5d) avg_chg_market_5d,
            avg(market_chg_10d) avg_chg_market_10d,
            avg(industry_chg_3d) avg_chg_industry_3d,
            avg(industry_chg_5d) avg_chg_industry_5d,
            avg(industry_chg_10d) avg_chg_industry_10d,
            cast(sum(case when ind_rank_perc_3d<=0.1 then 1.0 else 0 end)/
                  count(ind_rank_perc_3d) as double) top_ind_perc_3d,
            cast(sum(case when ind_rank_perc_5d<=0.1 then 1.0 else 0 end)/
                  count(ind_rank_perc_5d) as double) top_ind_perc_5d,
            cast(sum(case when ind_rank_perc_10d<=0.1 then 1.0 else 0 end)/
                  count(ind_rank_perc_10d) as double) top_ind_perc_10d,
            avg(stock_chg_3d) avg_chg_stock_3d,
            avg(stock_chg_5d) avg_chg_stock_5d,
            avg(stock_chg_10d) avg_chg_stock_10d,
            cast(sum(case when stock_rank_perc_3d<=0.1 then 1.0 else 0 end)/
                  count(stock_rank_perc_3d) as double) top_stock_perc_3d,
            cast(sum(case when stock_rank_perc_5d<=0.1 then 1.0 else 0 end)/
                  count(stock_rank_perc_5d) as double) top_stock_perc_5d,
            cast(sum(case when stock_rank_perc_10d<=0.1 then 1.0 else 0 end)/
                  count(stock_rank_perc_10d) as double) top_stock_perc_10d,
            '{1}' busi_date,
            {5} compute_term,
            trade_id
            from {6}.{7}
            where busi_date>='{0}' and busi_date<='{1}' and trade_type={2}
            GROUP BY trade_id
        """
        buySql = sql.format(startdate, enddate, 1, "avg_buy2low", "avg_high2buy",
                            compute, self.adata, self.stock_cust_trd_quant)
        buyBaseDf = self.sparkSession.sql(buySql)
        buyBaseDf = self._add_average(buyBaseDf, "avg_buy2low", "avg_high2buy", enddate, compute)
        buyBaseDf = self._cal_top(buyBaseDf)
        buyBaseDf.persist(StorageLevel.DISK_ONLY).count()
        return buyBaseDf

    def _add_average(self,df, low_col, high_col, enddate, compute_term):
        avgDf = df.agg(
            fn.avg(low_col).alias(low_col),
            fn.avg(high_col).alias(high_col),
            fn.avg("avg_chg_market_3d").alias("avg_chg_market_3d"),
            fn.avg("avg_chg_market_5d").alias("avg_chg_market_5d"),
            fn.avg("avg_chg_market_10d").alias("avg_chg_market_10d"),
            fn.avg("avg_chg_industry_3d").alias("avg_chg_industry_3d"),
            fn.avg("avg_chg_industry_5d").alias("avg_chg_industry_5d"),
            fn.avg("avg_chg_industry_10d").alias("avg_chg_industry_10d"),
            fn.avg("top_ind_perc_3d").alias("top_ind_perc_3d"),
            fn.avg("top_ind_perc_5d").alias("top_ind_perc_5d"),
            fn.avg("top_ind_perc_10d").alias("top_ind_perc_10d"),
            fn.avg("avg_chg_stock_3d").alias("avg_chg_stock_3d"),
            fn.avg("avg_chg_stock_5d").alias("avg_chg_stock_5d"),
            fn.avg("avg_chg_stock_10d").alias("avg_chg_stock_10d"),
            fn.avg("top_stock_perc_3d").alias("top_stock_perc_3d"),
            fn.avg("top_stock_perc_5d").alias("top_stock_perc_5d"),
            fn.avg("top_stock_perc_10d").alias("top_stock_perc_10d")
        ).withColumn("busi_date", fn.lit(enddate))\
            .withColumn("compute_term", fn.lit(compute_term))\
            .withColumn("trade_id", fn.lit("average"))
        df = df.union(avgDf)
        return df

    def _cal_top(self, df):
        # 如果按PEP8标准，改写成df.avg_chg_industry_3d is None 会spark会报错
        df = df.withColumn("top_ind_perc_3d",
                      fn.when(df.avg_chg_industry_3d == None, fn.lit(None)).
                      otherwise(df.top_ind_perc_3d)) \
            .withColumn("top_ind_perc_5d",
                        fn.when(df.avg_chg_industry_5d == None, fn.lit(None)).
                        otherwise(df.top_ind_perc_5d)) \
            .withColumn("top_ind_perc_10d",
                        fn.when(df.avg_chg_industry_10d == None, fn.lit(None)).
                        otherwise(df.top_ind_perc_10d)) \
            .withColumn("top_stock_perc_3d",
                        fn.when(df.avg_chg_stock_3d == None, fn.lit(None)).
                        otherwise(df.top_stock_perc_3d)) \
            .withColumn("top_stock_perc_5d",
                        fn.when(df.avg_chg_stock_5d == None, fn.lit(None)).
                        otherwise(df.top_stock_perc_5d)) \
            .withColumn("top_stock_perc_10d",
                        fn.when(df.avg_chg_stock_10d == None, fn.lit(None)).
                        otherwise(df.top_stock_perc_10d)) \
            .persist(storageLevel=StorageLevel.DISK_ONLY)
        return df

    def daily_compute(self, startdate=None, enddate=None, compute=90):
        """
        daily_compute
        """
        buyBaseDf = self._get_base_data(startdate, enddate, compute)
        dfBuyTime, dfBuyInd, dfBuyPrd = \
            self._cal(buyBaseDf, "avg_buy2low", "avg_high2buy", "asc", "desc")
        self.save_rank_table(compute, enddate,
                             dfBuyTime.repartition(20), self.stock_cust_diagnosis_buy_time)
        self.save_rank_table(compute, enddate,
                             dfBuyInd.repartition(20), self.stock_cust_diagnosis_buy_ind)
        self.save_rank_table(compute, enddate,
                             dfBuyPrd.repartition(20), self.stock_cust_diagnosis_buy_prd)

    def save_rank_table(self, compute, enddate, df, table):
        dropTablePartitionSql = \
            "alter table {0}.{1} drop if exists  partition(busi_date= '{2}',compute_term='{3}')" \
            .format(self.adata, table, enddate, compute)
        save_data(self.sparkSession, self.adata, table, enddate, df,
                  partitonByName=["busi_date", "compute_term"],
                  dropPartitonSql=dropTablePartitionSql)
