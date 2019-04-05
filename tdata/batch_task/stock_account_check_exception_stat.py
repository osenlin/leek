# coding=utf-8

from leek.tdata.batch_task.base_job import LeekSparkJob
from leek.common.util import save_data
from pyspark.storagelevel import StorageLevel

class StockAccountCheckExceptionStat(LeekSparkJob):
    """

    """

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.fdata = self.conf['feature_database']
        self.target_table = self.conf['stock_daily_check_report']

    def init_data(self):
        sql = """
          create table if not exists {0}.{1} (
                exception_pv	bigint	comment '出现异常的资产数',
                exception_uv	bigint	comment '出现异常的客户数',
                exception_pv_rate	double	comment '异常资产比例',
                exception_uv_rate	double	comment '异常客户比例',
                max_return	double	comment '最大异常收益',
                min_return	double	comment '最小异常收益',
                max_return_rate	double	comment '最大异常收益率',
                min_return_rate	double	comment '最小异常收益率',
                exception_type	string	comment '异常类型'
                )
                comment '账目核对统计与预警模块设计'
                partitioned by (busi_date   string comment '交易日期')
                STORED AS ORC
        """.format(self.fdata, self.conf['stock_daily_check_report'])
        print sql

    def daily_compute(self, startdate=None, enddate=None):
        sqlTmp = """
            select
                exception_pv,exception_uv,
                nvl(exception_pv/b.all_num,0.0) exception_pv_rate,
                nvl(exception_uv/b.all_user,0.0) exception_uv_rate,
                max_return,min_return,
                max_return_rate,min_return_rate,
                exception_type,
                a.busi_date
            from (
                select
                    count(1) exception_pv,
                    count(distinct trade_id) exception_uv,
                    max(return) max_return,
                    min(return) min_return,
                    max(return_rate) max_return_rate,
                    min(return_rate) min_return_rate,
                    case when qty_exception =1 and return_rate_exception=0 then 'qty'
                         when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                         else 'both' end exception_type,
                    busi_date
                from {2}.{3}
                where busi_date>= '{0}' and busi_date<='{1}' 
                      and (qty_exception<>0 or return_rate_exception<>0)
                group by case when qty_exception =1 and return_rate_exception=0 then 'qty'
                              when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                              else 'both' end,busi_date
            ) a inner join (
                select busi_date,count(1) all_num,count(distinct trade_id) all_user
                from {2}.{3}
                where busi_date>= '{0}' and busi_date<='{1}'
                GROUP BY busi_date
            ) b on a.busi_date=b.busi_date
        """.format(startdate, enddate, self.tdata, self.conf['check_data_table'])
        df = self.sparkSession.sql(sqlTmp).persist(StorageLevel.DISK_ONLY)
        self.batch_drop_partition(startdate, enddate,self.tdata, self.target_table)
        save_data(self.sparkSession, self.tdata, self.target_table, None, df,
                  defaultDropPartition=False)


if __name__ == '__main__':
    StockAccountCheckExceptionStat.logLevel = 'debug'
    StockAccountCheckExceptionStat("11").daily_compute("2017-04-10", "2017-04-10")
