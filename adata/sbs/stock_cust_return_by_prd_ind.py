# coding=utf-8


from leek.adata.sbs.stock_cust_return_by_prd import StockCustReturnByPrd
from pyspark.sql import functions as F, Row
from pyspark.storagelevel import StorageLevel


def _travel_ite(ite, busi_date, agg_return=True):
    for row in ite:
        yield _travel_row(row, busi_date, agg_return)


def _get_init_mkt_val(detail_list):
    long_init_mkt_val = 0.0
    short_end_debt_val = 0.0
    for item_long in detail_list:
        if item_long['trd_type'] == 'long_related':
            long_init_mkt_val = float(item_long['pre_mkt_val'])
            break
    for item_short in reversed(detail_list):
        if item_short['trd_type'] == 'short_related':
            short_end_debt_val = float(item_short['now_mkt_val'])
            break
    return long_init_mkt_val, short_end_debt_val


def _travel_row(row, busi_date, agg_return):
    dList = row.detail_list
    detail_list = sorted(dList, key=lambda x: x['busi_date'])
    long_init_mkt_val, short_end_debt_val = _get_init_mkt_val(detail_list)
    sum_long_return = 0.0
    sum_short_return = 0.0
    list_long_inv = [long_init_mkt_val]
    sum_long_pos_cash_flow = 0.0
    sum_long_neg_cash_flow = 0.0
    exception_label = 0
    for item in detail_list:
        return_val = float(item['return'])
        pos_cash_flow = float(item['pos_cash_flow'])
        neg_cash_flow = float(item['neg_cash_flow'])
        exception_label = max(exception_label, int(item['exception_label']))
        if int(item['exception_label']) > 0:
            return_val = 0
        if item['trd_type'] == 'short_related':
            sum_short_return += return_val
        else:
            sum_long_return += return_val
            sum_long_pos_cash_flow += pos_cash_flow
            sum_long_neg_cash_flow += neg_cash_flow
            lli_last = list_long_inv[-1]
            list_long_inv.append(lli_last + pos_cash_flow)
            list_long_inv.append(lli_last + pos_cash_flow + neg_cash_flow)
    long_inv = 0.0
    if len(list_long_inv) > 0:
        long_inv = max(list_long_inv)
    sum_return = sum_short_return + sum_long_return
    sum_short_pos_cash_flow = 0.0
    sum_short_neg_cash_flow = 0.0
    list_short_inv = [-short_end_debt_val]
    for item in reversed(detail_list):
        if item['trd_type'] != 'short_related':
            continue
        pos_cash_flow = float(item['pos_cash_flow'])
        neg_cash_flow = float(item['neg_cash_flow'])
        sum_short_pos_cash_flow += pos_cash_flow
        sum_short_neg_cash_flow += neg_cash_flow
        lsi_last = list_short_inv[-1]
        list_short_inv.append(pos_cash_flow + lsi_last)
        list_short_inv.append(pos_cash_flow + neg_cash_flow + lsi_last)
    short_inv = 0.0
    if len(list_short_inv) > 0:
        short_inv = max(list_short_inv)
    return_rate = sum_return / (long_inv + short_inv) if long_inv + short_inv != 0 else 0.0
    if agg_return:
        model = {"trade_id": row.trade_id, "busi_date": busi_date, "prd_ind": row.prd_ind,
                 "return": sum_return, "return_rate": return_rate,
                 "exception_label": exception_label}
    else:
        model = {"trade_id": row.trade_id, "busi_date": busi_date, "prd_ind": row.prd_ind,
                 "long_return": sum_long_return, "short_return": sum_short_return,
                 "long_inv": long_inv, "short_inv": short_inv, "exception_label": exception_label}
    return model


def _travel_interim_result(ite, busi_date):
    for row in ite:
        yield _travel_interim_result_row(row, busi_date)


def _travel_interim_result_row(row, busi_date):
    lst = row.list_return_rate
    lst = sorted(lst, key=lambda x: x["month"])
    long_return_accum = 0.0
    long_inv = 0.0
    for item in lst:
        _long_inv = float(item["long_inv"])
        _long_inv -= long_return_accum
        long_inv = max(long_inv, _long_inv)
        long_return_accum += float(item["long_return"])
    short_return_accum = 0.0
    short_inv = 0.0
    for item in reversed(lst):
        _short_inv = float(item["short_inv"])
        _short_inv -= short_return_accum
        short_inv = max(short_inv, _short_inv)
        short_return_accum += float(item["short_return"])
    return_rate = row.all_return / (long_inv + short_inv) if long_inv + short_inv != 0 else 0.0
    return {"trade_id": row.trade_id, "prd_ind": row.prd_ind, "busi_date": busi_date,
            "return": row.all_return, "return_rate": return_rate,
            "exception_label": row.exception_label}


class StockCustReturnByPrdInd(StockCustReturnByPrd):

    def __init__(self, spark):
        StockCustReturnByPrd.__init__(self, spark)
        self.target_table = self.conf["stock_cust_return_by_prd_ind_table"]
        self.step = int(self.confIni.get("task_stock_cust_return_by_prd_ind", "travel_part_step"))
        self.table_part = int(self.confIni.get("task_stock_cust_daily_holding", "part_numbers"))
        self.target_temp_table = self.conf["stock_cust_return_by_prd_ind_temp_table"]

    def init_data(self):
        sql = """
            create table if not exists {0}.{1}(
                trade_id  string,
                compute_term  bigint,
                busi_date  string,
                prd_ind  string,
                return  double,
                return_rate  double,
                exception_label  bigint,
                return_rank  double,
                return_rate_rank  bigint,
                return_ratio  double
            )
            comment '这个表将记录客户在给定周期内，各支股票上的收益情况（做多做空收益合并）'
            partitioned by (busi_date   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.target_table)
        # self.sparkSession.sql(sql)
        print sql

    def _get_base_data(self, startdate, enddate, part_start, part_end):
        sql = """
            select trade_id,prd_ind,trd_type,busi_date,
                    sum(pre_mkt_val) pre_mkt_val,
                    sum(now_mkt_val) now_mkt_val,
                    sum(pos_cash_flow) pos_cash_flow,
                    sum(neg_cash_flow) neg_cash_flow,
                    max(exception_label) exception_label,
                    sum(return) return
            from {2}.{3}
            where busi_date>='{0}' and busi_date<='{1}' and part>='{4}' and part<='{5}'
            and prd_no!='0.0'
            GROUP  by trade_id,prd_ind,trd_type,busi_date
        """.format(startdate, enddate, self.adata, self.stock_cust_daily_holding, part_start,
                   part_end)
        df = self.sparkSession.sql(sql).withColumn("detail_item",
                                                   F.create_map(F.lit("pre_mkt_val"), "pre_mkt_val",
                                                                F.lit("now_mkt_val"), "now_mkt_val",
                                                                F.lit("pos_cash_flow"),
                                                                "pos_cash_flow",
                                                                F.lit("neg_cash_flow"),
                                                                "neg_cash_flow",
                                                                F.lit("exception_label"),
                                                                "exception_label",
                                                                F.lit("trd_type"), "trd_type",
                                                                F.lit("return"), "return",
                                                                F.lit("busi_date"),
                                                                "busi_date"))\
            .groupBy("trade_id", "prd_ind")\
            .agg(F.collect_list("detail_item").alias("detail_list"))
        df.persist(StorageLevel.DISK_ONLY).count()
        return df

    def _create_prefinaldf_df(self, dfBase, enddate):
        # 调用类外的方法，无法实现overwrite的机制，需要复制和超类相同的代码进行
        dfBaseRdd = dfBase.rdd.mapPartitions(lambda ite: _travel_ite(ite, enddate), 100)
        if dfBaseRdd is None or dfBaseRdd.isEmpty():
            print "dfBaseRdd empty,after _travel_row2 "
            return
        return self.sparkSession.createDataFrame(dfBaseRdd)

    def _create_agg_by_month(self, dfBase, enddate):
        # 调用类外的方法，无法实现overwrite的机制，需要复制和超类相同的代码进行
        dfBaseRdd = dfBase.rdd.mapPartitions(lambda ite: _travel_ite(ite, enddate, False), 100)
        if dfBaseRdd is None or dfBaseRdd.isEmpty():
            print "dfBaseRdd empty,after _travel_row2 "
            return
        return self.sparkSession.createDataFrame(dfBaseRdd)

    def _get_interim_result(self, save_db, save_table, month, busi_date):
        """
        获取中间结果
        :param save_db:
        :param save_table:
        :param month:
        :param busi_date:
        :return:
        """
        sql = """
            select trade_id,prd_ind,sum(long_return + short_return) all_return,
            max(exception_label) exception_label,
            collect_list(
                    str_to_map(concat(
                      ',long_return:',long_return,
                      ',short_return:',short_return,
                      ',long_inv:',long_inv,
                      ',short_inv:',short_inv,
                      ',month:',month
                      ),",",":"
                    )
                  ) list_return_rate
            from {0}.{1} t
            where t.month>='{2}' and t.month<='{3}'
            group by trade_id,prd_ind
        """.format(save_db, save_table, month, busi_date[0: 7])
        df = self.sparkSession.sql(sql)
        return self.sparkSession.createDataFrame(
            df.rdd.mapPartitions(lambda ite: _travel_interim_result(ite, busi_date), 100))
