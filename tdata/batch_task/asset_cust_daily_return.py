# coding=utf-8
from leek.tdata.batch_task.base_job import LeekSparkJob
from leek.common.util import get_date, save_data


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
        print '不需要init'

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
            nvl(a.mkt_val,0)+nvl(b.mkt_val,0) net_asset,nvl(a.busi_date,b.busi_date) busi_date,
            nvl(a.cash_mkt_val,0)+nvl(b.cash_mkt_val,0) cash_val,
            nvl(a.stock_mkt_val,0)+nvl(b.stock_mkt_val,0) stock_mkt_val
            from (
                select trade_id, sum(mkt_val) mkt_val,busi_date,
                       sum(case when prd_no='0.0' then mkt_val else 0 end) cash_mkt_val,
                       sum(case when prd_no!='0.0' then mkt_val else 0 end) stock_mkt_val
                from {1}.{2}
                where busi_date >='{0}' and busi_date<='{4}' 
                group by trade_id,busi_date
            ) a full outer join (
                select trade_id, sum(mkt_val) mkt_val,busi_date,
                       sum(case when prd_no='0.0' then mkt_val else 0 end) cash_mkt_val,
                       sum(case when prd_no!='0.0' then mkt_val else 0 end) stock_mkt_val
                from {1}.{3}
                where busi_date >='{0}' and busi_date<='{4}'
                group by trade_id,busi_date
            ) b on a.trade_id=b.trade_id and a.busi_date=b.busi_date 
            
        """
        selectSql1 = sqlTmp1.format(startdate, self.odata, self.stock_asset_holding,
                                    self.stock_debt_holding, enddate)
        tempTable1 = "cust_daily_asset_debt_tmp"
        self._reg_table(selectSql1, tempTable1)
        return tempTable1

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
                   nvl(debt,0) debt,
                   nvl(capital_in,0) capital_in,
                   nvl(capital_out,0) capital_out,
                   nvl(cash_capital_in,0) cash_capital_in,
                   nvl(stock_capital_in,0) stock_capital_in,
                   nvl(cash_capital_out,0) cash_capital_out,
                   nvl(stock_capital_out,0) stock_capital_out,
                   nvl(cash_val,0) cash_val,
                   nvl(stock_mkt_val,0) stock_mkt_val,
                   nvl(int_tax_in,0) int_tax_in,
                   nvl(int_tax_out,0) int_tax_out,
                   nvl(fa.busi_date,c.busi_date) busi_date
            from {3} fa
            full outer join (
                    select trade_id,busi_date,
                      sum(capital_in) capital_in,
                      sum(capital_out) capital_out,
                      sum(case when prd_no ='0.0' then capital_in else 0 end ) cash_capital_in,
                      sum(case when prd_no!='0.0' then capital_in else 0 end ) stock_capital_in,
                      sum(case when prd_no ='0.0' then capital_out else 0 end ) cash_capital_out,
                      sum(case when prd_no!='0.0' then capital_out else 0 end ) stock_capital_out,
                      sum(nvl(int_tax_in,0)) int_tax_in,
                      sum(nvl(int_tax_out,0)) int_tax_out
                    from {1}.{2}
                    where busi_date >='{0}' and busi_date<='{4}'
                    group by trade_id,busi_date
            ) c on fa.trade_id=c.trade_id and fa.busi_date=c.busi_date
        """
        selectSql2 = sqlTmp2.format(startdate, self.tdata, self.stock_cash_flow, asset_debt_table,
                                    enddate)
        tempTable2 = "cust_daily_capital_in_out_tmp"
        self._reg_table(selectSql2, tempTable2)
        return tempTable2

    def _cal_return_inv(self, capital_in_out_table, startdate=None, enddate=None):
        """
        计算T日 return和inv
        :param capital_in_out_table:
        :param startdate:
        :param enddate:
        :return:
        """
        #startdate = get_date(self.date_order, self.order_date, startdate, -1)
        sqlGetBeginDate = """
            select a.*,startdate from {2}  a
            inner JOIN (
              select busi_date,lag(busi_date) over(order by busi_date) startdate
              from {3}
            ) b on a.busi_date=b.busi_date
            where a.busi_date>='{0}' and a.busi_date<='{1}'
        """.format(startdate, enddate, capital_in_out_table,
                   "{0}.{1}".format(self.odata, self.trading_calendar))
        tempGetBeginDataTable = "get_begindate_cap_in_out_tmp"
        self._reg_table(sqlGetBeginDate, tempGetBeginDataTable)
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
                      (nvl(pre_net_asset,0) + nvl(capital_in,0) + nvl(capital_out,0)) 
                      - nvl(int_tax_in,0)-nvl(int_tax_out,0) return_type_2,
                    {1} inv,
                    nvl(ffc.busi_date,lastdate) busi_date,
                    nvl(cash_capital_in,0) cash_capital_in,
                    nvl(cash_capital_out,0) cash_capital_out,
                    nvl(stock_capital_in,0) stock_capital_in,
                    nvl(stock_capital_out,0) stock_capital_out,
                    nvl(cash_val,0) cash_val,
                    nvl(stock_mkt_val,0) stock_mkt_val,
                    nvl(d.pre_debt,0) pre_debt,
                    nvl(d.pre_asset,0) pre_asset,
                    nvl(d.pre_net_asset,0) pre_net_asset,
                    nvl(d.pre_cash_val,0) pre_cash_val,
                    nvl(d.pre_stock_mkt_val,0) pre_stock_mkt_val
            from {0} ffc 
            full outer join (
                select trade_id,net_asset pre_net_asset,
                        asset pre_asset,
                        debt pre_debt,
                        cash_val pre_cash_val,
                        stock_mkt_val pre_stock_mkt_val,busi_date,lastdate
                from {0}  st inner JOIN (
                  select busi_date bdate,lead(busi_date) over(order by busi_date) lastdate
                  from {3}
                ) b on st.busi_date=b.bdate
                where st.busi_date<'{2}'
            ) d on ffc.trade_id=d.trade_id and ffc.startdate=d.busi_date
        """
        selectSql3 = sqlTmp3.format(
            tempGetBeginDataTable,
            """
            greatest(greatest(nvl(pre_asset,0) + nvl(pre_debt,0),-1*nvl(pre_debt,0)) 
                        + nvl(capital_in,0)+ nvl(int_tax_in,0),
                        greatest(nvl(pre_asset,0) + nvl(pre_debt,0),-1*nvl(pre_debt,0)) 
                         + nvl(capital_in,0) + nvl(capital_out,0) + nvl(int_tax_in,0)
                          + nvl(int_tax_out,0),
                        greatest(nvl(pre_asset,0) + nvl(pre_debt,0),-1*nvl(pre_debt,0))
            )
            """, enddate, "{0}.{1}".format(self.odata, self.trading_calendar))
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
                    fffd.trade_id,asset,debt,net_asset,
                    pre_debt,
                    pre_asset,
                    pre_net_asset,
                    pre_cash_val,
                    pre_stock_mkt_val,
                    capital_in,capital_out,
                    cash_capital_in,stock_capital_in,cash_val,stock_mkt_val,
                    cash_capital_out,stock_capital_out,
                    nvl(e.qty_exception, 0) exception_label,
                    int_tax_in,
                    int_tax_out,
                    case when e.trade_id is null or e.qty_exception=1
                         then 0 else return_type_1 end return_type_1,
                    case when e.trade_id is null or e.qty_exception=1
                         then 0 else return_type_2 end return_type_2,
                    case when e.trade_id is null or
                              e.qty_exception=1 or inv=0
                         then 0 else (return_type_1 - int_tax_in-int_tax_out)/inv 
                    end return_rate_type_1,
                    case when e.trade_id is null or
                              e.qty_exception=1 or inv=0
                         then 0 else return_type_2/inv 
                    end return_rate_type_2,
                    fffd.busi_date
                from {3} fffd  left outer join(
                    select trade_id,max(qty_exception) qty_exception,
                    max(return_rate_exception) return_rate_exception,busi_date
                    from {1}.{2}
                    where busi_date >='{0}' and busi_date <='{4}'
                    group by trade_id,busi_date
                ) e on fffd.trade_id=e.trade_id and fffd.busi_date=e.busi_date
        """
        selectSql4 = sqlTmp4.format(startdate, self.tdata, self.stock_daily_check_data,
                                    cust_daily_return_inv_table, enddate)
        return self.sparkSession.sql(selectSql4)

    def daily_compute(self, startdate=None, enddate=None):
        asset_debt_table = self._cal_asset_debt(startdate, enddate)
        # 需要获取计算日前一天的数据
        capital_in_out_table = self._cal_capital_in_out(asset_debt_table, startdate, enddate)
        cust_daily_return_inv_table = self._cal_return_inv(capital_in_out_table, startdate, enddate)
        finalDf = self._cal_final_res(cust_daily_return_inv_table, startdate, enddate)
        self.batch_drop_partition(startdate, enddate, self.tdata, self.asset_cust_daily_return)
        save_data(self.sparkSession, self.tdata, self.asset_cust_daily_return, None, finalDf,
                  defaultDropPartition=False)