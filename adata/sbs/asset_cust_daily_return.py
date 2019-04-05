# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import get_date


class AssetCustDailyReturn(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_daily_return = self.conf['stock_cust_daily_return_table']
        self.stock_asset_holding = self.conf["asset_table"]
        self.stock_debt_holding = self.conf["debt_table"]
        self.stock_daily_check_data = self.conf["check_data_table"]
        self.stock_cash_flow = self.conf["stock_cash_flow_merge_table"]
        self.asset_cust_daily_return = self.conf["asset_cust_daily_return_table"]

    def init_data(self):
        """
        需要预先创建表，在daily_compute中sql查询会有依赖这张表
        :return:
        """
        sql = """
            create table if not exists {0}.{1} (
                trade_id string,
                asset double,
                debt double,
                net_asset double,
                capital_in double,
                capital_out double,
                exception_label bigint,
                return_type_1 double,
                return_type_2 double,
                return_rate_type_1 double,
                return_rate_type_2 double,
                int_tax_in double,
                int_tax_out double
            )
            comment '记录客户每日在股票市场上的持仓和每支股票的收益情况'
            partitioned by (busi_date   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.asset_cust_daily_return)
        print sql
        self.sparkSession.sql(sql)

    def _reg_table(self, sql, table, numPartitons=None):
        df1 = self.sparkSession.sql(sql)
        print 'reg table:', table
        if self.logLevel != 'debug':
            if numPartitons:
                df1.repartition(numPartitons).createOrReplaceTempView(table)
            else:
                df1.createOrReplaceTempView(table)

    def _cal_asset_debt(self, startdate=None, enddate=None):
        """
        T日的asset，debt，net_asset
        :param startdate:
        :param enddate:
        :return:
        """
        sqlTmp1 = """
            select nvl(a.trade_id,b.trade_id) trade_id,nvl(a.mkt_val,0) asset,nvl(b.mkt_val,0) debt,
            nvl(a.mkt_val,0)+nvl(b.mkt_val,0) net_asset
            from (
                select trade_id, sum(mkt_val) mkt_val
                from {1}.{2}
                where busi_date='{0}'
                group by trade_id
            ) a full outer join (
                select trade_id, sum(mkt_val) mkt_val
                from {1}.{3}
                where busi_date='{0}'
                group by trade_id
            ) b on a.trade_id=b.trade_id
        """
        selectSql1 = sqlTmp1.format(enddate, self.odata, self.stock_asset_holding,
                                    self.stock_debt_holding)
        tempTable1 = "cust_daily_asset_debt_tmp"
        self._reg_table(selectSql1, tempTable1)

        startdate = get_date(self.date_order, self.order_date, enddate, -1)
        selectSql2 = sqlTmp1.format(startdate, self.odata, self.stock_asset_holding,
                                    self.stock_debt_holding)
        tempTable2 = "cust_daily_asset_debt_before_tmp"
        self._reg_table(selectSql2, tempTable2)
        return tempTable1, tempTable2

    def _cal_capital_in_out(self, asset_debt_table, startdate=None, enddate=None):
        """
        计算T日的asset，debt，net_asset，capital_in，capital_out
        :param asset_debt_table:
        :param startdate:
        :param enddate:
        :return:
        """
        sqlTmp2 = """
            select nvl(fa.trade_id,c.trade_id) trade_id,nvl(asset,0) asset,
                   nvl(net_asset,0) net_asset,
                   nvl(debt,0) debt,nvl(capital_in,0) capital_in,nvl(capital_out,0) capital_out,
                   nvl(int_tax_in,0) int_tax_in,
                   nvl(int_tax_out,0) int_tax_out
            from {3} fa
            full outer join (
                    select trade_id,sum(capital_in) capital_in,sum(capital_out) capital_out,
                           sum(nvl(int_tax_in,0)) int_tax_in,
                           sum(nvl(int_tax_out,0)) int_tax_out
                    from {1}.{2}
                    where busi_date='{0}'
                    group by trade_id
            ) c on fa.trade_id=c.trade_id
        """
        selectSql2 = sqlTmp2.format(enddate, self.fdata, self.stock_cash_flow, asset_debt_table)
        tempTable2 = "cust_daily_capital_in_out_tmp"
        self._reg_table(selectSql2, tempTable2)
        return tempTable2

    def _cal_return_inv(self, capital_in_out_table, asset_debt_before_table):
        """
        计算T日 return和inv
        :param capital_in_out_table:
        :param startdate:
        :param enddate:
        :return:
        """
        sqlTmp3 = """
            select  nvl(ffc.trade_id,d.trade_id) trade_id,
                    nvl(asset,0) asset,
                    nvl(debt,0) debt,
                    nvl(net_asset,0) net_asset,
                    nvl(capital_in,0) capital_in,
                    nvl(capital_out,0) capital_out,
                    nvl(int_tax_in,0) int_tax_in,
                    nvl(int_tax_out,0) int_tax_out,
                    nvl(net_asset,0)-
                      (nvl(pre_net_asset,0) + nvl(capital_in,0) + nvl(capital_out,0)) return_type_1,
                    nvl(net_asset,0)-
                      (nvl(pre_net_asset,0) + nvl(capital_in,0) + nvl(capital_out,0))-
                       nvl(int_tax_in,0)- nvl(int_tax_out,0) return_type_2,
                    {2} inv
            from {1} ffc full outer join (
                select trade_id,net_asset pre_net_asset,asset pre_asset,debt pre_debt
                from {0}
            ) d on ffc.trade_id=d.trade_id
        """
        selectSql3 = sqlTmp3.format(asset_debt_before_table, capital_in_out_table,
                                    """
            greatest(greatest(nvl(pre_asset,0) + nvl(pre_debt,0),-1*nvl(pre_debt,0)) 
                        + nvl(capital_in,0)+ nvl(int_tax_in,0),
                        greatest(nvl(pre_asset,0) + nvl(pre_debt,0),-1*nvl(pre_debt,0)) 
                         + nvl(capital_in,0) + nvl(capital_out,0) + nvl(int_tax_in,0)
                          + nvl(int_tax_out,0),
                        greatest(nvl(pre_asset,0) + nvl(pre_debt,0),-1*nvl(pre_debt,0))
            )
            """)
        tempTable3 = "cust_daily_return_inv_tmp"
        self._reg_table(selectSql3, tempTable3)
        return tempTable3

    def _cal_final_res(self, cust_daily_return_inv_table, startdate=None, enddate=None):
        """
        计算exception_label,return,和return_rate
        :param cust_daily_return_inv_table:
        :param startdate:
        :param enddate:
        :return:
        """
        sqlTmp4 = """
                select
                    fffd.trade_id,asset,debt,net_asset,capital_in,capital_out,
                    nvl(e.qty_exception, 0) exception_label,
                    case when e.trade_id is null or e.qty_exception=1
                         then 0 else return_type_1 end return_type_1,
                    case when e.trade_id is null or e.qty_exception=1
                         then 0 else return_type_2 end return_type_2,
                    case when e.trade_id is null or
                              e.qty_exception=1 or inv=0
                         then 0 else (return_type_1-int_tax_in-int_tax_out)/inv 
                    end return_rate_type_1,
                    case when e.trade_id is null or
                              e.qty_exception=1 or inv=0
                         then 0 else return_type_2/inv 
                    end return_rate_type_2,
                    int_tax_in,
                    int_tax_out
                from {3} fffd  left outer join(
                    select trade_id,max(qty_exception) qty_exception,
                    max(return_rate_exception) return_rate_exception
                    from {1}.{2}
                    where busi_date='{0}'
                    group by trade_id
                ) e on fffd.trade_id=e.trade_id
        """
        selectSql4 = sqlTmp4.format(enddate, self.fdata, self.stock_daily_check_data,
                                    cust_daily_return_inv_table)
        tempTable4 = "cust_daily_final_tmp"
        self._reg_table(selectSql4, tempTable4, 20)
        return tempTable4

    def daily_compute(self, startdate=None, enddate=None):
        asset_debt_table, asset_debt_before_table = self._cal_asset_debt(startdate, enddate)
        # 需要获取计算日前一天的数据
        capital_in_out_table = self._cal_capital_in_out(asset_debt_table, startdate, enddate)
        cust_daily_return_inv_table = self._cal_return_inv(capital_in_out_table,
                                                           asset_debt_before_table)
        final_res = self._cal_final_res(cust_daily_return_inv_table, startdate, enddate)

        self.sparkSession.sql(
            " alter table {1}.{2} add if not exists  partition(busi_date= '{0}') "
            .format(enddate, self.adata, self.asset_cust_daily_return))
        insertSqlTmp = """
            insert {0} table {1}.{2}  partition(busi_date='{3}')
            select * from {4}
        """
        insertSql = insertSqlTmp.format("overwrite", self.adata, self.asset_cust_daily_return,
                                        enddate, final_res)
        self.sparkSession.sql(insertSql)

        if self.logLevel != 'debug':
            self.sparkSession.catalog.dropTempView(asset_debt_table)
            self.sparkSession.catalog.dropTempView(asset_debt_before_table)
            self.sparkSession.catalog.dropTempView(capital_in_out_table)
            self.sparkSession.catalog.dropTempView(cust_daily_return_inv_table)
            self.sparkSession.catalog.dropTempView(final_res)


if __name__ == '__main__':
    AssetCustDailyReturn.logLevel = 'debug'
    AssetCustDailyReturn(None).init_data()
