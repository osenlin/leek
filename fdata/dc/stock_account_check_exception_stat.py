# coding=utf-8

from leek.fdata.dc.leekjob import LeekSparkJob


class StockAccountCheckExceptionStat(LeekSparkJob):
    """

    """

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.fdata = self.conf['feature_database']

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

        self.sparkSession.sql(sql)

    def daily_compute(self, startdate=None, enddate=None):
        sqlPrePartition = " alter table {1}.{2} add if not exists  partition(busi_date= '{0}') "\
            .format(enddate, self.fdata, self.conf['stock_daily_check_report'])

        self.sparkSession.sql(sqlPrePartition)

        sqlTmp = """
            select
                exception_pv,exception_uv,
                exception_pv/b.all_num exception_pv_rate,
                exception_uv/b.all_user exception_uv_rate,
                max_return,min_return,
                max_return_rate,min_return_rate,
                exception_type
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
                         else 'both' end exception_type
                from {1}.{2}
                where  busi_date= '{0}' and (qty_exception<>0 or return_rate_exception<>0)
                group by case when qty_exception =1 and return_rate_exception=0 then 'qty'
                              when qty_exception =0 and return_rate_exception=1 then 'return_rate'
                              else 'both' end
            ) a cross join (
                select count(1) all_num,count(distinct trade_id) all_user
                from {1}.{2}
                where busi_date= '{0}'
            ) b
        """.format(enddate, self.fdata, self.conf['check_data_table'])

        df = self.sparkSession.sql(sqlTmp)
        temptable = "check_exception_stat_temp"
        df.repartition(20).createOrReplaceTempView(temptable)
        self.sparkSession.sql("""
            insert overwrite table {1}.{2} partition(busi_date= '{0}')
            select * from {3}
        """.format(enddate, self.fdata, self.conf['stock_daily_check_report'], temptable))
        self.sparkSession.sql(
            "refresh table %s.%s" % (self.fdata, self.conf['stock_daily_check_report']))
        self.sparkSession.catalog.dropTempView(temptable)


if __name__ == '__main__':
    StockAccountCheckExceptionStat.logLevel = 'debug'
    StockAccountCheckExceptionStat("11").daily_compute("2017-04-10", "2017-04-10")
