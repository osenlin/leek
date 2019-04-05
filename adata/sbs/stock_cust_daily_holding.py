# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data
from pyspark.storagelevel import StorageLevel


class StockCustDailyHolding(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_daily_holding = self.conf['stock_cust_daily_holding_table']
        self.stock_daily_check_data = self.conf["check_data_table"]
        self.stock_description = self.conf["stock_description_table"]
        self.part_numbers = int(self.confIni.get("task_stock_cust_daily_holding", "part_numbers"))

    def init_data(self):
        sql = """
            create table if not exists {0}.{1} (
                trade_id  string,
                prd_no  string,
                prd_name  string,
                prd_ind  string,
                trd_type  string,
                pre_mkt_val  double,
                now_mkt_val  double,
                pos_cash_flow  double,
                neg_cash_flow  double,
                capital_in  double,
                capital_out  double,
                exception_label  bigint,
                return  double,
                return_rate  double,
                part int,
            )
            comment '记录客户每日在股票市场上的持仓和每支股票的收益情况'
            partitioned by (busi_date   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.stock_cust_daily_holding)
        print sql  # self.sparkSession.sql(sql)

    def daily_compute(self, startdate=None, enddate=None):
        # self.sparkSession.sql(
        #     " alter table {1}.{2} add if not exists  partition(busi_date= '{0}') ".format(
        #         enddate, self.adata, self.stock_cust_daily_holding))
        sql = """
            select
                a.trade_id,
                a.prd_no,
                nvl(b.name_short, 'unknown') prd_name,
                nvl(b.ind_name_level4, 'unknown') prd_ind,
                trd_type,
                pre_mkt_val,
                now_mkt_val,
                pos_cash_flow,
                neg_cash_flow,
                capital_in,
                capital_out,
                nvl(qty_exception, 0) exception_label,
                case when a.prd_no='0.0' or qty_exception=1
                     then 0 else return end return,
                case when a.prd_no='0.0' or qty_exception=1
                     then 0 else {4} end return_rate,
                busi_date,
                pmod(hash(a.trade_id),{7}) part
            from (
                select
                    trade_id,busi_date,prd_no,trd_type,
                    sum(pre_mkt_val) pre_mkt_val,
                    sum(now_mkt_val) now_mkt_val,
                    sum(pos_cash_flow) pos_cash_flow,
                    sum(neg_cash_flow) neg_cash_flow,
                    sum(trd_cash_flow) trd_cash_flow,
                    sum(capital_in) capital_in,
                    sum(capital_out) capital_out,
                    max(qty_exception) qty_exception,
                    max(return_rate_exception) return_rate_exception,
                    sum(return) return
                from {2}.{3}
                where busi_date='{0}' and trd_type='{1}'
                group by trade_id,busi_date,prd_no,trd_type
            ) a left outer join {5}.{6} b on a.prd_no=b.prd_no
        """

        longSql = sql.format(enddate, "long_related", self.fdata, self.stock_daily_check_data,
                             "(now_mkt_val-pre_mkt_val-trd_cash_flow)/(pos_cash_flow+pre_mkt_val)",
                             self.odata, self.stock_description, self.part_numbers)
        dfLong = self.sparkSession.sql(longSql)
        dfLong.persist(StorageLevel.DISK_ONLY).count()
        shortSql = sql.format(enddate, "short_related", self.fdata, self.stock_daily_check_data,
                              "(now_mkt_val-pre_mkt_val-trd_cash_flow)/(pos_cash_flow-now_mkt_val)",
                              self.odata, self.stock_description, self.part_numbers)
        dfShort = self.sparkSession.sql(shortSql)
        dfFinal = dfLong.union(dfShort).repartition(20)
        save_data(self.sparkSession, self.adata, self.stock_cust_daily_holding, enddate, dfFinal,
                  partitonByName=["busi_date", "part"])
