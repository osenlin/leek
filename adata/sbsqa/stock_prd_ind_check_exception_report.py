# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data
from pyspark.storagelevel import StorageLevel


class StockPrdIndCheckExceptionReport(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_prd_ind_check_exception_data = self.conf[
            'stock_prd_ind_check_exception_data_table']
        self.stock_prd_ind_check_exception_report = self.conf[
            "stock_prd_ind_check_exception_report_table"]
        self.stock_cust_return_by_prd = self.conf['stock_cust_return_by_prd_table']
        self.stock_cust_return_by_prd_ind = self.conf["stock_cust_return_by_prd_ind_table"]

    def init_data(self):
        print 'init stock_ac_check_exception_report'

    def _get_all_num(self, enddate, database, table):
        sql = """
            select compute_term,sum(all_num) all_num from(
                select trade_id,prd_ind,compute compute_term,1 all_num
                from {1}.{2}
                where busi_date= '{0}'
                group by trade_id,prd_ind,compute
            ) a
            GROUP by compute_term
        """.format(enddate, database, table)
        return self.sparkSession.sql(sql)

    def daily_compute(self, startdate=None, enddate=None):
        dfPrd = self._get_all_num(enddate, self.adata, self.stock_cust_return_by_prd)
        dfInd = self._get_all_num(enddate, self.adata, self.stock_cust_return_by_prd_ind)
        sqlTmp = """
            select busi_date,compute_term,
                sum(prd_exception_label) prd_exception_uv,
                sum(ind_exception_label) ind_exception_uv,
                sum(re_equal_exception) re_equal_exception_uv,
                sum(prd_pos_neg_exception) prd_pos_neg_exception_uv,
                sum(ind_pos_neg_exception) ind_pos_neg_exception_uv
            from  {1}.{2}
            where busi_date= '{0}'
            group by busi_date,compute_term
        """.format(enddate, self.adata, self.stock_prd_ind_check_exception_data)
        df = self.sparkSession.sql(sqlTmp)
        df = df.join(dfPrd, ["compute_term"], "inner")\
            .select(df["*"], (df.prd_exception_uv/dfPrd.all_num).alias("prd_exception_rate"),
                    (df.re_equal_exception_uv / dfPrd.all_num).alias("re_equal_exception_rate"),
                    (df.prd_pos_neg_exception_uv / dfPrd.all_num)
                    .alias("prd_pos_neg_exception_rate")
                    )\
            .persist(StorageLevel.DISK_ONLY)
        df = df.join(dfInd, ["compute_term"], "inner")\
            .select(df["*"], (df.ind_exception_uv/dfInd.all_num).alias("ind_exception_rate"),
                    (df.ind_pos_neg_exception_uv / dfInd.all_num)
                    .alias("ind_pos_neg_exception_rate")
                    )\
            .persist(StorageLevel.DISK_ONLY).repartition(5)
        save_data(self.sparkSession, self.adata, self.stock_prd_ind_check_exception_report,
                  enddate, df)
