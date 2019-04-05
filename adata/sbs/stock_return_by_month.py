# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from leek.common.util import save_data


def _travel_row2(row, update_date=None):
    dList = row.detail_list
    detailList = sorted(dList, key=lambda x: x['busi_date'])
    init_asset_val = float(detailList[0]['pre_asset_val'])
    init_debt = float(detailList[0]['pre_debt_val'])
    current_asset_val = float(detailList[-1]['now_asset_val'])
    current_debt = float(detailList[-1]['now_debt_val'])
    init_inv = max(init_asset_val + init_debt, -1 * init_debt)
    sum_long = 0.0
    sum_short = 0.0
    sum_total = 0.0
    sum_capital_in = 0.0
    sum_capital_out = 0.0
    sum_int_tax_in = 0.0
    sum_int_tax_out = 0.0
    exception_label = 0
    list_inv = [init_inv]
    long_return_detail = []
    short_return_detail = []
    for item in detailList:
        item_busi_date = item['busi_date']
        long_return = float(item['long_return'])
        short_return = float(item['short_return'])
        total_return = float(item['total_return'])
        capital_in = float(item['capital_in'])
        capital_out = float(item['capital_out'])
        exception_label_item = int(item['exception_label'])
        return_int_tax_in = float(item['int_tax_in'])
        return_int_tax_out = float(item['int_tax_out'])
        if int(item['exception_label']) > 0:
            exception_label = exception_label | exception_label_item
            # long_return = 0
            # short_return = 0
            # total_return = 0
        sum_long += long_return
        sum_int_tax_in += return_int_tax_in
        sum_int_tax_out += return_int_tax_out
        sum_short += short_return
        sum_total += total_return
        sum_capital_in += capital_in
        sum_capital_out += capital_out
        list_inv_last = list_inv[-1]
        list_inv.append(list_inv_last + capital_in + return_int_tax_in)
        list_inv.append(list_inv_last + capital_in + capital_out +
                        return_int_tax_in + return_int_tax_out)
        long_return_detail.append({"busi_date": item_busi_date, "return": str(long_return)})
        short_return_detail.append({"busi_date": item_busi_date, "return": str(short_return)})
    maxInv = max(list_inv)
    total_asset_return_type_1 = current_asset_val + current_debt - \
                                (init_asset_val + init_debt + sum_capital_in + sum_capital_out)
    total_asset_return_type_2 = current_asset_val + current_debt - \
                                (init_asset_val + init_debt + sum_capital_in + sum_capital_out) - \
                                sum_int_tax_in - sum_int_tax_out
    total_asset_return_rate_type_1 = (total_asset_return_type_1 - sum_int_tax_in -
                                      sum_int_tax_out) / maxInv if maxInv != 0 else 0.0
    total_asset_return_rate_type_2 = total_asset_return_type_2 / maxInv if maxInv != 0 else 0.0
    long_return_rate = sum_long / maxInv if maxInv != 0 else 0.0
    short_return_rate = sum_short / maxInv if maxInv != 0 else 0.0
    total_return_rate = sum_total / maxInv if maxInv != 0 else 0.0
    model = dict(trade_id=row.trade_id, month=str(detailList[0]['busi_date'])[0:7],
                 update_date=update_date, init_asset_val=init_asset_val,
                 current_asset_val=current_asset_val, init_debt=init_debt,
                 current_debt=current_debt, capital_in=sum_capital_in, capital_out=sum_capital_out,
                 total_asset_return_type_1=total_asset_return_type_1,
                 total_asset_return_type_2=total_asset_return_type_2,
                 total_asset_return_rate_type_1=total_asset_return_rate_type_1,
                 total_asset_return_rate_type_2=total_asset_return_rate_type_2,
                 int_tax_in=sum_int_tax_in,int_tax_out=sum_int_tax_out,
                 stock_long_return=sum_long, stock_long_return_rate=long_return_rate,
                 stock_short_return=sum_short, stock_short_return_rate=short_return_rate,
                 stock_total_return=sum_total, stock_total_return_rate=total_return_rate,
                 exception_label=exception_label, stock_long_return_detail=long_return_detail,
                 stock_short_return_detail=short_return_detail)
    return model


class StockReturnByMonth(LeekSparkJob):
    """
    每个月1号算，如果1号是周末还计算？
    """

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_daily_return = self.conf['stock_cust_daily_return_table']
        self.stock_return_by_month = self.conf["stock_return_by_month_table"]

    def init_data(self):
        sql = """
            create table if not exists {0}.{1}(
                    trade_id string,
                    update_date string,
                    init_asset_val double,
                    current_asset_val double,
                    init_debt double,
                    current_debt double,
                    capital_in double,
                    capital_out double,
                    total_asset_return double,
                    total_asset_return_rate double,
                    stock_long_return double,
                    stock_long_return_rate double,
                    stock_short_return double,
                    stock_short_return_rate double,
                    stock_total_return double,
                    stock_total_return_rate double,
                    exception_label bigint,
                    total_return_rank_percentage string,
                    stock_long_return_detail array<map<string,string>>,
                    stock_short_return_detail array<map<string,string>>,
                    stock_profit_user_ratio double
            )
            comment '这个表将记录客户每个月的收益情况'
            partitioned by (month   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.stock_return_by_month)
        print sql

    def _get_base_data(self, startdate, enddate):
        sql = """
                  select  trade_id,collect_list(detail_item) detail_list from (
                    select trade_id,
                        (str_to_map(concat(
                        'busi_date:',busi_date,
                        ',pre_asset_val:',pre_asset_val,
                        ',pre_debt_val:',pre_debt_val,
                        ',now_asset_val:',now_asset_val,
                        ',now_debt_val:',now_debt_val,
                        ',capital_in:',capital_in,
                        ',capital_out:',capital_out,
                        ',exception_label:',exception_label,
                        ',long_return:',long_return,
                        ',total_return:',total_return,
                        ',short_return:',short_return,
                        ',int_tax_in:',int_tax_in,
                        ',int_tax_out:',int_tax_out
                        ),",",":")) detail_item
                    from {2}.{3}
                    where busi_date>='{0}' and busi_date<='{1}'
                  )  a
                  group by trade_id
                """
        selectSql = sql.format(startdate, enddate, self.adata, self.stock_cust_daily_return)
        return self.sparkSession.sql(selectSql)

    def daily_compute(self, startdate=None, enddate=None):
        """
        daily_compute
        :param startdate:
        :param enddate:
        :return:
        """
        # 月度计算，需要从一号开始
        dfBase = self._get_base_data(startdate, enddate)
        # 计算long_return，short_return，total_return
        dfBaseRdd = dfBase.rdd.map(lambda row: _travel_row2(row, enddate))
        if dfBaseRdd is None or dfBaseRdd.isEmpty():
            print "dfBaseRdd empty,after _travel_row2 "
            return
        preFinalDF = self.sparkSession.createDataFrame(dfBaseRdd)
        tempFinalTable = "pre_final_month_table"
        preFinalDF.createOrReplaceTempView(tempFinalTable)
        profit_num = preFinalDF.where(preFinalDF.stock_long_return >= 0).count()
        all_num = preFinalDF.count()
        finalDf = self.sparkSession.sql("""
          select *,rank() over(order by stock_total_return desc)/{2} stock_total_return_rank_percentage,
            {0} stock_profit_user_ratio
          from {1}
           """.format(profit_num * 1.0 / all_num, tempFinalTable, all_num)).repartition(20)
        # drop parition if exists
        dropPartitionSql = "alter table {0}.{1} drop if exists partition(month='{2}')".format(
            self.adata, self.stock_return_by_month, enddate[0:7])
        save_data(self.sparkSession, self.adata, self.stock_return_by_month, enddate[0:7], finalDf,
                  "month", dropPartitionSql)
        if self.logLevel != 'debug':
            self.sparkSession.catalog.dropTempView(tempFinalTable)
