# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, full_outer_join
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as fn

LOG_LEVEL = 'info'


class StockTrdrrCheckException(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_close_o_trade_long_data = self.conf['long_close_trade_table']
        self.stock_unclose_o_trade_long_data = self.conf['long_unclose_trade_table']
        self.stock_close_o_trade_short_data = self.conf['short_close_trade_table']
        self.stock_unclose_o_trade_short_data = self.conf['short_unclose_trade_table']
        self.stock_close_c_trade_long_data = self.conf['long_c_close_trade_table']
        self.stock_unclose_c_trade_long_data = self.conf['long_c_unclose_trade_table']
        self.stock_close_c_trade_short_data = self.conf['short_c_close_trade_table']
        self.stock_unclose_c_trade_short_data = self.conf['short_c_unclose_trade_table']
        self.stock_close_tradebyday_long_data = self.conf["stock_close_tradebyday_long_data_table"]
        self.stock_close_tradebyday_short_data = self.conf[
            "stock_close_tradebyday_short_data_table"]
        self.stock_unclose_tradebyday_long_data = self.conf[
            "stock_unclose_tradebyday_long_data_table"]
        self.stock_unclose_tradebyday_short_data = self.conf[
            "stock_unclose_tradebyday_short_data_table"]
        self.stock_close_prd_long_data = self.conf["long_close_table"]
        self.stock_unclose_prd_long_data = self.conf["long_unclose_table"]
        self.stock_close_prd_short_data = self.conf["short_close_table"]
        self.stock_unclose_prd_short_data = self.conf["short_unclose_table"]
        self.taget_table = self.conf["stock_trdrr_check_exception_data_table"]

    def init_data(self):
        print 'init StockTrdrrCheckException'

    def _cal_data(self, busi_date, database, long_table, short_table, max_rr, min_rr, max_wt,
                  min_wt, wt='weighted_term'):
        sqlTmp = """
            select busi_date,exception_label,
                   max(return_rate) {3},
                   min(return_rate) {4},
                   max({7}) {5},
                   min({7}) {6},
                   '{8}' trd_type
            from {1}.{2}
            where busi_date='{0}'
            GROUP BY busi_date,exception_label
        """
        sql_long = sqlTmp.format(busi_date, database, long_table, max_rr, min_rr, max_wt, min_wt,
                                 wt, 'long_related')
        if LOG_LEVEL == 'debug':
            print sql_long
        dfLong = self.sparkSession.sql(sql_long)
        sql_short = sqlTmp.format(busi_date, database, short_table, max_rr, min_rr, max_wt,
                                  min_wt, wt, 'short_related')
        if LOG_LEVEL == 'debug':
            print sql_short
        dfShort = self.sparkSession.sql(sql_short)
        return dfLong.union(dfShort).persist(StorageLevel.DISK_ONLY)

    def daily_compute(self, startdate=None, enddate=None):
        df_o_close = self._cal_data(enddate, self.fdata, self.stock_close_o_trade_long_data,
                                    self.stock_close_o_trade_short_data, 'o_close_rr_max',
                                    'o_close_rr_min', 'o_close_wt_max', 'o_close_wt_min')
        df_o_unclose = self._cal_data(enddate, self.fdata, self.stock_unclose_o_trade_long_data,
                                      self.stock_unclose_o_trade_short_data,
                                      'o_unclose_rr_max', 'o_unclose_rr_min', 'o_unclose_wt_max',
                                      'o_unclose_wt_min')
        df1 = full_outer_join(df_o_close, df_o_unclose,
                              ["busi_date", "exception_label",
                               "trd_type"]).persist(StorageLevel.DISK_ONLY)
        df_c_close = self._cal_data(enddate, self.fdata, self.stock_close_c_trade_long_data,
                                    self.stock_close_c_trade_short_data, 'c_close_rr_max',
                                    'c_close_rr_min', 'c_close_wt_max', 'c_close_wt_min')
        df_c_unclose = self._cal_data(enddate, self.fdata, self.stock_unclose_c_trade_long_data,
                                      self.stock_unclose_c_trade_short_data,
                                      'c_unclose_rr_max', 'c_unclose_rr_min', 'c_unclose_wt_max',
                                      'c_unclose_wt_min')
        df2 = full_outer_join(df_c_close, df_c_unclose,
                              ["busi_date", "exception_label",
                               "trd_type"]).persist(StorageLevel.DISK_ONLY)
        df_day_close = self._cal_data(enddate, self.fdata, self.stock_close_tradebyday_long_data,
                                      self.stock_close_tradebyday_short_data,
                                      'tbd_close_rr_max', 'tbd_close_rr_min', 'tbd_close_wt_max',
                                      'tbd_close_wt_min')
        df_day_unclose = self._cal_data(enddate, self.fdata,
                                        self.stock_unclose_tradebyday_long_data,
                                        self.stock_unclose_tradebyday_short_data,
                                        'tbd_unclose_rr_max', 'tbd_unclose_rr_min',
                                        'tbd_unclose_wt_max', 'tbd_unclose_wt_min')
        df3 = full_outer_join(df_day_close, df_day_unclose,
                              ["busi_date", "exception_label",
                               "trd_type"]).persist(StorageLevel.DISK_ONLY)
        df_prd_close = self._cal_data(enddate, self.fdata, self.stock_close_prd_long_data,
                                      self.stock_close_prd_short_data, 'prd_close_rr_max',
                                      'prd_close_rr_min', 'prd_close_wt_max', 'prd_close_wt_min',
                                      wt='holding_term')
        df_prd_unclose = self._cal_data(enddate, self.fdata, self.stock_unclose_prd_long_data,
                                        self.stock_unclose_prd_short_data,
                                        'prd_unclose_rr_max', 'prd_unclose_rr_min',
                                        'prd_unclose_wt_max', 'prd_unclose_wt_min',
                                        wt='holding_term')
        df4 = full_outer_join(df_prd_close, df_prd_unclose,
                              ["busi_date", "exception_label",
                               "trd_type"]).persist(StorageLevel.DISK_ONLY)
        df12 = full_outer_join(df1, df2, ["busi_date", "exception_label", "trd_type"])
        df34 = full_outer_join(df3, df4, ["busi_date", "exception_label", "trd_type"])
        df = full_outer_join(df12, df34,
                             ["busi_date", "exception_label",
                              "trd_type"]).persist(StorageLevel.DISK_ONLY)
        df.registerTempTable("tmp_stocktrdrrcheckexception")
        sql = """
            select exception_label,
            trd_type,
            busi_date,
            nvl(o_close_rr_max,0) o_close_rr_max,
            nvl(o_close_rr_min,0) o_close_rr_min,
            nvl(o_unclose_rr_max,0) o_unclose_rr_max,
            nvl(o_unclose_rr_min,0) o_unclose_rr_min,
            nvl(c_close_rr_max,0) c_close_rr_max,
            nvl(c_close_rr_min,0) c_close_rr_min,
            nvl(c_unclose_rr_max,0) c_unclose_rr_max,
            nvl(c_unclose_rr_min,0) c_unclose_rr_min,
            nvl(tbd_close_rr_max,0) tbd_close_rr_max,
            nvl(tbd_close_rr_min,0) tbd_close_rr_min,
            nvl(tbd_unclose_rr_max,0) tbd_unclose_rr_max,
            nvl(tbd_unclose_rr_min,0) tbd_unclose_rr_min,
            nvl(prd_close_rr_max,0) prd_close_rr_max,
            nvl(prd_close_rr_min,0) prd_close_rr_min,
            nvl(prd_unclose_rr_max,0) prd_unclose_rr_max,
            nvl(prd_unclose_rr_min,0) prd_unclose_rr_min,
            nvl(o_close_wt_max,0) o_close_wt_max,
            nvl(o_close_wt_min,0) o_close_wt_min,
            nvl(o_unclose_wt_max,0) o_unclose_wt_max,
            nvl(o_unclose_wt_min,0) o_unclose_wt_min,
            nvl(c_close_wt_max,0) c_close_wt_max,
            nvl(c_close_wt_min,0) c_close_wt_min,
            nvl(c_unclose_wt_max,0) c_unclose_wt_max,
            nvl(c_unclose_wt_min,0) c_unclose_wt_min,
            nvl(tbd_close_wt_max,0) tbd_close_wt_max,
            nvl(tbd_close_wt_min,0) tbd_close_wt_min,
            nvl(tbd_unclose_wt_max,0) tbd_unclose_wt_max,
            nvl(tbd_unclose_wt_min,0) tbd_unclose_wt_min,
            nvl(prd_close_wt_max,0) prd_close_wt_max,
            nvl(prd_close_wt_min,0) prd_close_wt_min,
            nvl(prd_unclose_wt_max,0) prd_unclose_wt_max,
            nvl(prd_unclose_wt_min,0) prd_unclose_wt_min
            from tmp_stocktrdrrcheckexception
        """
        finalDf = self.sparkSession.sql(sql).repartition(5)
        save_data(self.sparkSession, self.adata, self.taget_table, enddate, finalDf)
