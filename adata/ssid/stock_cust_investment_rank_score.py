# coding=utf-8
from leek.adata.ssid.ssidjob import LeekSparkJob
from leek.common.util import save_data
from pyspark.sql import Row
from pyspark.storagelevel import StorageLevel
import numpy as np


class StockCustInvestRankScore(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_investment_ability = self.conf['stock_cust_investment_ability_table']
        self.stock_cust_investment_rank_score = self.conf['stock_cust_investment_rank_score_table']
        self.index_price = self.conf["index_price_table"]

    def init_data(self):
        pass

    def _get_base_data(self, startdate, enddate, compute_term):
        sql = """
                    select
                    trade_id,return_rate,re_variance,up_capture,
                    down_capture,alpha,beta,cl_value,exception_label
                    from {1}.{2}
                    where busi_date='{0}' and compute_term='{3}'
                """
        selectSql = sql.format(enddate, self.adata, self.stock_cust_investment_ability,
                               compute_term)
        return self.sparkSession.sql(selectSql)

    def _get_rm_data(self, startdate, enddate):
        sql = """
            select collect_list(str_to_map(concat(
                            'busi_date:',busi_date,
                            ',daily_return_rate:',daily_return_rate
                            ),",",":")) detail_list
            from {2}.{3}
            where prd_no='000300.SH' and busi_date>='{0}' and busi_date<='{1}'
        """
        selectSql = sql.format(startdate, enddate, self.adata, self.index_price)
        return self.sparkSession.sql(selectSql)

    def _cal_rm_data(self, spark, df, schema):
        def _travel_row_rm(row):
            dList = row.detail_list
            detail_list = sorted(dList, key=lambda x: x['busi_date'])
            return_rate = 1.0
            return_rate_list = []
            for item in detail_list:
                item_m_return_rate = float(item['daily_return_rate'])
                return_rate *= (1 + item_m_return_rate)
                return_rate_list.append(return_rate)
            re_variance = np.var(return_rate_list).item()
            model = {"trade_id": "average", "return_rate": return_rate - 1,
                     "re_variance": re_variance, "up_capture": 1.0, "down_capture": 1.0,
                     "alpha": 0.0, "beta": 1.0, "cl_value": 0.0, "exception_label": 0}
            return Row(**model)

        # 填入_get_base_data得到的DF.schema
        return spark.createDataFrame(df.rdd.map(lambda row: _travel_row_rm(row)), schema)

    def daily_compute(self, startdate=None, enddate=None, compute=7):
        """
        daily_compute
        :param startdate:
        :param enddate:
        :return:
        """
        # calc 2 return，return_rate
        dfBase = self._get_base_data(startdate, enddate, compute)
        dfBase.persist(StorageLevel.DISK_ONLY).count()
        averageTempDf = self._get_rm_data(startdate, enddate)
        averageDf = self._cal_rm_data(self.sparkSession, averageTempDf, dfBase.schema)
        averageDf.persist(StorageLevel.DISK_ONLY).count()
        self._cal_final_result(dfBase, averageDf, compute, enddate)

    def _cal_final_result(self, dfBase, averageDf, compute, enddate):
        prefinalDf = self._return_rate_rank_perc(self.sparkSession, dfBase, averageDf, compute,
                                                 enddate)
        prefinalDf.persist(StorageLevel.DISK_ONLY).count()
        dropPartitionSql = """alter table {0}.{1} drop if exists
                              partition(busi_date= '{2}',compute_term='{3}')"""\
            .format(self.adata, self.stock_cust_investment_rank_score, enddate, compute)
        save_data(self.sparkSession, self.adata, self.stock_cust_investment_rank_score, enddate,
                  prefinalDf, partitonByName=["busi_date", "compute_term"],
                  dropPartitonSql=dropPartitionSql)

    def _return_rate_rank_perc(self, spark, dfBase, averageDf, compute, endate):
        tempTable = "temp_rank_score"
        allDataDf = dfBase.union(averageDf)
        allDataDf.createOrReplaceTempView(tempTable)
        # 补上averageDf 1条数据
        all_num = float(allDataDf.count())
        sql = """
            select trade_id,compute_term,busi_date,
                    cast(return_rate_rank as double) return_rate_rank,
                    cast(re_variance_rank as double) re_variance_rank,
                    cast(up_capture_rank as double) up_capture_rank,
                    cast(down_capture_rank as double) down_capture_rank,
                    cast(alpha_rank as double) alpha_rank,
                    cast(cl_value_rank as double) cl_value_rank,
                    cast((1-return_rate_rank)*100 as double) return_rate_score,
                    cast((1-re_variance_rank)*100 as double) re_variance_score,
                    cast((1-up_capture_rank)*100  as double) up_capture_score,
                    cast((1-down_capture_rank)*100 as double) down_capture_score,
                    cast((1-alpha_rank)*100 as double) alpha_score,
                    cast((1-cl_value_rank)*100 as double) cl_value_score,
                    cast((6-(return_rate_rank+re_variance_rank+
                      up_capture_rank+down_capture_rank+alpha_rank+cl_value_rank))*100/6 as double)
                      total_score,
                    exception_label
            from (
                select
                  trade_id,
                  {1} compute_term,
                  '{3}' busi_date,
                  rank() over(order by return_rate desc)/{2} return_rate_rank,
                  rank() over(order by re_variance)/{2} re_variance_rank,
                  rank() over(order by up_capture desc)/{2} up_capture_rank,
                  rank() over(order by down_capture)/{2} down_capture_rank,
                  rank() over(order by alpha desc)/{2} alpha_rank,
                  rank() over(order by cl_value desc)/{2} cl_value_rank,
                  exception_label
                from {0}
            ) a
        """.format(tempTable, compute, all_num, endate)
        spark.sql(sql).createOrReplaceTempView("temp_total_rank")
        sqlTotalRank = """
            select *,rank() over(order by total_score desc) total_rank
            from temp_total_rank
        """
        return spark.sql(sqlTotalRank)
