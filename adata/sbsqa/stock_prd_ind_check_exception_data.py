# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data, get_date
from pyspark.sql import functions as fn
from pyspark.storagelevel import StorageLevel


class StockPrdIndCheckExceptionData(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_prd_ind_check_exception_data = self.conf[
            'stock_prd_ind_check_exception_data_table']
        self.stock_cust_return_by_prd = self.conf['stock_cust_return_by_prd_table']
        self.stock_cust_return_by_prd_ind = self.conf["stock_cust_return_by_prd_ind_table"]

    def init_data(self):
        print 'init stock_ac_check_exception_data'

    def daily_compute(self, startdate=None, enddate=None):
        sqlPrd = """
            select trade_id,prd_ind,
            busi_date,compute compute_term,
            max(prd_name) prd_name,
            sum(return) prd_return,
            max(case when return>=0 and return_rate>=0 then 0
                     when return<0 and return_rate<0 then 0
                     else 1 end) prd_pos_neg_exception,
            max(exception_label) prd_exception_label
            from {1}.{2}
            where busi_date='{0}' and prd_no!='0.0'
            group by trade_id,prd_ind,compute,busi_date
        """.format(enddate, self.adata, self.stock_cust_return_by_prd)
        dfPrd = self.sparkSession.sql(sqlPrd)
        sqlInd = """
            select trade_id b_trade_id,prd_ind b_prd_ind,
            compute b_compute_term,
            sum(return) ind_return,
            max(case when return>=0 and return_rate>=0 then 0
                     when return<0 and return_rate<0 then 0
                     else 1 end) ind_pos_neg_exception,
            max(exception_label) ind_exception_label
            from {1}.{2}
            where  busi_date='{0}'
            group by trade_id,prd_ind,compute
        """.format(enddate, self.adata, self.stock_cust_return_by_prd_ind)
        dfInd = self.sparkSession.sql(sqlInd)
        cond = [dfPrd.trade_id == dfInd.b_trade_id,
                dfPrd.prd_ind == dfInd.b_prd_ind,
                dfPrd.compute_term == dfInd.b_compute_term]
        df = dfPrd.join(dfInd, cond, "full_outer")\
            .select(dfPrd.busi_date, dfPrd.trade_id, dfPrd.prd_ind,
                    dfPrd.prd_name, dfPrd.compute_term,
                    dfPrd.prd_pos_neg_exception, dfInd.ind_pos_neg_exception,
                    dfPrd.prd_exception_label, dfInd.ind_exception_label,
                    fn.when(dfPrd.prd_return-dfInd.ind_return <= 0.01, 0)
                    .otherwise(1).alias("re_equal_exception")
                    )\
            .persist(StorageLevel.DISK_ONLY)
        df = df.where("""
            prd_pos_neg_exception>0 or ind_pos_neg_exception>0 or prd_exception_label>0
            or ind_exception_label>0 or re_equal_exception>0
        """).repartition(5)
        save_data(self.sparkSession, self.adata, self.stock_prd_ind_check_exception_data,
                  enddate, df)
