# coding=utf-8
from leek.adata.ssid.ssidjob import LeekSparkJob
from pyspark.sql import functions as F
from leek.common.util import save_data
from pyspark.storagelevel import StorageLevel


class StockCustTrdQuant(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cash_flow_detail = self.conf["cash_flow_table"]
        self.stock_price = self.conf["stock_price_table"]
        self.stock_shortterm_return_rate = self.conf["stock_shortterm_return_rate_table"]
        self.index_shortterm_return_rate = self.conf["index_shortterm_return_rate_table"]
        self.stock_description = self.conf["stock_description_table"]
        self.index_description = self.conf["index_description_table"]
        self.stock_cust_trd_quant = self.conf["stock_cust_trd_quant_table"]

    def init_data(self):
        sql = """
            create table if not exists {0}.{1}(
                trade_id  string "客户代码",
                prd_no  string "股票代码",
                timestamp  long "时间戳",
                trade_type  bigint "操作类型",
                trdprice2low  double "交易价格到日内最低价的比例",
                high2trdprice  double "交易价格到日内最高价的比例",
                stock_chg_3d  double "3天内股票涨幅",
                stock_chg_5d  double "5天内股票涨幅",
                stock_chg_10d  double "10天内股票涨幅",
                stock_rank_perc_3d  double "3天内股票涨幅排名",
                stock_rank_perc_5d  double "5天内股票涨幅排名",
                stock_rank_perc_10d  double "10天内股票涨幅排名",
                market_chg_3d  double "3天内大盘指数涨幅",
                market_chg_5d  double "5天内大盘指数涨幅",
                market_chg_10d  double "10天内大盘指数涨幅",
                industry_ind_3d  double "3天内板块指数涨幅",
                industry_chg_5d  double "5天内板块指数涨幅",
                industry_chg_10d  double "10天内板块指数涨幅",
                ind_rank_perc_3d  double "3天内所在板块涨幅排名",
                ind_rank_perc_5d  double "5天内所在板块涨幅排名",
                ind_rank_perc_10d  double "10天内所在板块涨幅排名"
            )
            comment '记录客户盈亏情况'
            partitioned by (busi_date   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.stock_cust_trd_quant)
        print sql

    def _get_base_info(self, startdate, enddate):
        sql = """
            select
                oa.trade_id,
                oa.prd_no,
                case when oa.trd_qty >=0 then 1 else 2 end trade_type,
                timestamp,
                case when low_price is not null
                     then (trd_price-low_price)/low_price else 0 end trdprice2low,
                case when high_price is not null
                     then (high_price-trd_price)/high_price else 0 end high2trdprice,
                re_p3d stock_chg_3d,
                re_p5d stock_chg_5d,
                re_p10d stock_chg_10d,
                rank_perc_p3d stock_rank_perc_3d,
                rank_perc_p5d stock_rank_perc_5d,
                rank_perc_p10d stock_rank_perc_10d,
                oa.busi_date
            from (
              select * from {2}.{3}
              where prd_no!='0.0' and trd_cash_flow!=0 and trd_qty != 0 and
               busi_date>='{0}' and busi_date <='{1}'
            ) oa
            left outer  join (
              select low_price,high_price,prd_no from {2}.{4}
              where busi_date>='{0}' and busi_date <='{1}'
            ) ob on oa.prd_no=ob.prd_no
            left outer join(
              select * from {5}.{6}
              where busi_date>='{0}' and busi_date <='{1}'
            ) fa on oa.prd_no=fa.prd_no
        """
        selectSql = sql.format(startdate, enddate, self.odata, self.stock_cash_flow_detail,
                               self.stock_price, self.fdata, self.stock_shortterm_return_rate)
        return self.sparkSession.sql(selectSql)

    def _get_market_value(self, startdate, enddate):
        sql = """
            select re_p3d,re_p5d,re_p10d from {2}.{3}
            where  busi_date>='{0}' and busi_date <='{1}' and prd_no='000300.SH'
        """
        selectSql = sql.format(startdate, enddate, self.fdata, self.index_shortterm_return_rate)
        return self.sparkSession.sql(selectSql)

    def _get_industry_prd_no(self, startdate, enddate):
        sql = """
            select oa.prd_no prd_no,ob.prd_no index_no,
                  fa.re_p3d,fa.re_p5d,fa.re_p10d,
                  fa.rank_perc_p3d,fa.rank_perc_p5d,fa.rank_perc_p10d
            from {odata}.{stock_description} oa
            inner join {odata}.{index_description} ob
            on oa.ind_code_level3=ob.ind_code
            inner join {fdata}.{index_shortterm_return_rate} fa
            on fa.prd_no=ob.prd_no and fa.busi_date='{busi_date}'
        """
        selectSql = sql.format(odata=self.odata, fdata=self.fdata,
                               stock_description=self.stock_description,
                               index_description=self.index_description,
                               index_shortterm_return_rate=self.index_shortterm_return_rate,
                               busi_date=enddate)
        return self.sparkSession.sql(selectSql)

    def daily_compute(self, startdate=None, enddate=None):
        """
        daily_compute
        """
        dfBase = self._get_base_info(startdate, enddate)
        dfBase.persist(StorageLevel.DISK_ONLY).count()
        dfMarketValue = self._get_market_value(startdate, enddate)
        dfMarketValue.persist(StorageLevel.DISK_ONLY).count()
        dfBase = dfBase\
            .withColumn("market_chg_3d", F.lit(dfMarketValue.first().re_p3d))\
            .withColumn("market_chg_5d", F.lit(dfMarketValue.first().re_p5d))\
            .withColumn("market_chg_10d", F.lit(dfMarketValue.first().re_p10d))
        dfIndustryPrdNo = self._get_industry_prd_no(startdate, enddate)
        dfIndustryPrdNo.persist(StorageLevel.DISK_ONLY).count()
        finalDf = self._merge_market_value(dfBase, dfIndustryPrdNo).repartition(20)
        save_data(self.sparkSession, self.adata, self.stock_cust_trd_quant, enddate, finalDf)

    def _merge_market_value(self, dfBase, dfIndustryPrdNo):
        dfIndustryPrdNo = dfIndustryPrdNo.withColumn("prd_no_a", dfIndustryPrdNo.prd_no)
        cond = [dfBase.prd_no == dfIndustryPrdNo.prd_no_a]
        tmp1 = dfBase.join(dfIndustryPrdNo, cond, "left_outer").\
            select(dfBase["*"],
                   dfIndustryPrdNo.re_p3d.alias("industry_chg_3d"),
                   dfIndustryPrdNo.re_p5d.alias("industry_chg_5d"),
                   dfIndustryPrdNo.re_p10d.alias("industry_chg_10d"),
                   dfIndustryPrdNo.rank_perc_p3d.alias("ind_rank_perc_3d"),
                   dfIndustryPrdNo.rank_perc_p5d.alias("ind_rank_perc_5d"),
                   dfIndustryPrdNo.rank_perc_p10d.alias("ind_rank_perc_10d"))
        return tmp1
