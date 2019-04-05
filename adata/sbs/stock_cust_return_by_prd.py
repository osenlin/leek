# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob, func_logging
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from leek.common.util import save_data, get_date_by_month_diff


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


def _travel_ite(ite, busi_date, agg_return=True):
    for row in ite:
        yield _travel_row(row, busi_date, agg_return)


def _travel_row(row, busi_date, agg_return):
    # detail_list = row.detail_list
    dList = row.detail_list
    detail_list = sorted(dList, key=lambda x: x['busi_date'])
    long_init_mkt_val, short_end_debt_val = _get_init_mkt_val(detail_list)
    sum_long_return = 0.0
    sum_short_return = 0.0
    list_long_inv = [long_init_mkt_val]
    sum_long_pos_cash_flow = 0.0
    sum_long_neg_cash_flow = 0.0
    prd_ind = ''
    prd_name = ''
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
        prd_ind = item['prd_ind']
        prd_name = item['prd_name']
    long_inv = 0.0
    if len(list_long_inv) > 0:
        long_inv = max(list_long_inv)
    sum_return = sum_short_return + sum_long_return
    sum_short_pos_cash_flow = 0.0
    sum_short_neg_cash_flow = 0.0
    list_short_inv = [- short_end_debt_val]
    for item in reversed(detail_list):
        if item['trd_type'] != 'short_related':
            continue
        pos_cash_flow = float(item['pos_cash_flow'])
        neg_cash_flow = float(item['neg_cash_flow'])
        sum_short_pos_cash_flow += pos_cash_flow
        sum_short_neg_cash_flow += neg_cash_flow
        lsi_last = list_short_inv[-1]
        list_short_inv.append(pos_cash_flow + lsi_last)
        list_short_inv.append(neg_cash_flow + pos_cash_flow + lsi_last)
    short_inv = 0.0
    if len(list_short_inv) > 0:
        short_inv = max(list_short_inv)
    return_rate = sum_return / (long_inv + short_inv) if long_inv + short_inv != 0 else 0.0
    if agg_return:
        model = {"trade_id": row.trade_id, "busi_date": busi_date, "prd_no": row.prd_no,
                 "prd_ind": prd_ind, "prd_name": prd_name, "return": sum_return,
                 "return_rate": return_rate, "exception_label": exception_label}
    else:
        model = {"trade_id": row.trade_id, "busi_date": busi_date, "prd_no": row.prd_no,
                 "prd_ind": prd_ind, "prd_name": prd_name, "long_return": sum_long_return,
                 "short_return": sum_short_return, "long_inv": long_inv, "short_inv": short_inv,
                 "exception_label": exception_label}
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
        prd_name = item["prd_name"]
        prd_ind = item["prd_ind"]
    short_return_accum = 0.0
    short_inv = 0.0
    for item in reversed(lst):
        _short_inv = float(item["short_inv"])
        _short_inv -= short_return_accum
        short_inv = max(short_inv, _short_inv)
        short_return_accum += float(item["short_return"])
    return_rate = row.all_return / (long_inv + short_inv) if long_inv + short_inv != 0 else 0.0
    return {"trade_id": row.trade_id, "prd_no": row.prd_no, "prd_ind": prd_ind,
            "prd_name": prd_name, "busi_date": busi_date, "return": row.all_return,
            "return_rate": return_rate, "exception_label": row.exception_label}


class StockCustReturnByPrd(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_daily_holding = self.conf['stock_cust_daily_holding_table']
        self.target_table = self.conf["stock_cust_return_by_prd_table"]
        self.step = int(self.confIni.get("task_stock_cust_return_by_prd", "travel_part_step"))
        self.table_part = int(self.confIni.get("task_stock_cust_daily_holding", "part_numbers"))
        self.target_temp_table = self.conf["stock_cust_return_by_prd_temp_table"]

    def init_data(self):
        sql = """
            create table if not exists {0}.{1} (
                trade_id  string,
                compute_term  bigint,
                busi_date  string,
                prd_no  string,
                prd_ind  string,
                prd_name string,
                return  double,
                return_rate  double,
                exception_label  bigint,
                return_rank  double,
                return_rate_rank  bigint,
                return_ratio  double,
                part bigint
            )
            comment '这个表将记录客户在给定周期内，各支股票上的收益情况（做多做空收益合并）'
            partitioned by (busi_date   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.target_table)
        # self.sparkSession.sql(sql)
        print sql

    @func_logging(level='info')
    def _get_base_data(self, startdate, enddate, part_start, part_end):
        sql = """
            select trade_id,prd_no,trd_type,busi_date,
                    sum(pre_mkt_val) pre_mkt_val,
                    sum(now_mkt_val) now_mkt_val,
                    sum(pos_cash_flow) pos_cash_flow,
                    sum(neg_cash_flow) neg_cash_flow,
                    max(exception_label) exception_label,
                    sum(return) return,
                    max(prd_ind) prd_ind,
                    max(prd_name) prd_name
            from {2}.{3}
            where busi_date>='{0}' and busi_date<='{1}' and part>='{4}' and part<='{5}'
            and prd_no!='0.0'
            GROUP  by trade_id,prd_no,trd_type,busi_date
        """.format(startdate, enddate, self.adata, self.stock_cust_daily_holding, part_start,
                   part_end)
        df = self.sparkSession.sql(sql)\
            .withColumn("detail_item",
                        F.create_map(F.lit("pre_mkt_val"),
                                     "pre_mkt_val", F.lit("now_mkt_val"), "now_mkt_val",
                                     F.lit("pos_cash_flow"), "pos_cash_flow",
                                     F.lit("neg_cash_flow"), "neg_cash_flow",
                                     F.lit("exception_label"), "exception_label",
                                     F.lit("trd_type"), "trd_type", F.lit("return"), "return",
                                     F.lit("prd_ind"), "prd_ind", F.lit("prd_name"), "prd_name",
                                     F.lit("busi_date"), "busi_date"))\
            .groupBy("trade_id", "prd_no")\
            .agg(F.collect_list("detail_item").alias("detail_list"))
        df.persist(StorageLevel.DISK_ONLY).count()
        return df

    def daily_compute(self, startdate=None, enddate=None, compute=7):
        # step 2的整数倍
        if compute == 7:
            step = self.step
            part_start = 0
            part_end = part_start + step - 1
            dropParition = True
            while part_end < self.table_part:
                print 'part start[{}] end[{}],dropParition[{}] table[{}]'.format(part_start,
                                                                                 part_end,
                                                                                 dropParition,
                                                                                 self.target_table)
                dfBase = self._get_base_data(startdate, enddate, part_start, part_end)
                preFinalDf = self._create_prefinaldf_df(dfBase, enddate)
                if preFinalDf is None:
                    return
                self._cal_rank_raterank_ratio(preFinalDf, enddate, self.adata, self.target_table,
                                              compute, dropParition)
                self._update_daily_interim_result(self.adata, self.target_temp_table, enddate,
                                                  part_start, part_end)
                part_start = part_end + 1
                part_end = part_start + step - 1
                dropParition = False
        else:
            month_diff = compute / 30
            month_ago = get_date_by_month_diff(enddate, month_diff).strftime("%Y-%m")
            preFinalDf = self._get_interim_result(self.adata, self.target_temp_table, month_ago,
                                                  enddate)
            if preFinalDf is None:
                return
            self._cal_rank_raterank_ratio(preFinalDf, enddate, self.adata, self.target_table,
                                          compute)

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
            select trade_id,prd_no,sum(long_return + short_return) all_return,
            max(exception_label) exception_label,
                  collect_list(
                    str_to_map(concat(
                      'prd_ind:',prd_ind,
                      ',prd_name:',prd_name,
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
            group by trade_id,prd_no
        """.format(save_db, save_table, month, busi_date[0: 7])
        df = self.sparkSession.sql(sql)
        dfFinal = self.sparkSession.createDataFrame(
            df.rdd.mapPartitions(lambda ite: _travel_interim_result(ite, busi_date), 100))
        dfFinal.persist(StorageLevel.DISK_ONLY).count()
        return dfFinal

    def _update_daily_interim_result(self, save_db, save_table, enddate=None, part_start=None,
                                     part_end=None):
        """
        更新中间结果
        :param save_db:
        :param save_table:
        :param enddate:
        :param part_start:
        :param part_end:
        :return:
        """
        month = enddate[0:7]
        month_start_date = month + '-01'
        dfInterBase = self._get_base_data(month_start_date, enddate, part_start, part_end)
        interimResultDf = self._create_agg_by_month(dfInterBase, enddate)
        interimResultDf.persist(StorageLevel.DISK_ONLY).count()
        dropSql = "alter table %s.%s drop if exists partition(month='%s')" % (
            save_db, save_table, month)
        save_data(self.sparkSession, save_db, save_table, enddate,
                  interimResultDf.withColumn('month', F.lit(month)), partitonByName="month",
                  dropPartitonSql=dropSql)

    @func_logging(level='info')
    def _cal_rank_raterank_ratio(self, preFinalDf, enddate, save_db, save_table, compute,
                                 dropParition=True):
        """
        计算收益排行，收益率排行，以及收益占比，逻辑互通多周期可通用
        :param preFinalDf:
        :param enddate:
        :param save_db:
        :param save_table:
        :param compute:
        :param dropParition:
        :return:
        """
        if preFinalDf is None:
            return
        # 在time weighted 情况下有可能出现return和return_rate 不同向
        profitDf = self._get_profit_percent(preFinalDf, "return >= 0")
        notProfitDf = self._get_profit_percent(preFinalDf, "return < 0")
        # calc profit percent
        tmp1 = self._cal_return_ratio(preFinalDf, profitDf, " return>=0")
        tmp1.persist(StorageLevel.DISK_ONLY).count()
        tmp2 = self._cal_return_ratio(preFinalDf, notProfitDf, " return <0")
        tmp2.persist(StorageLevel.DISK_ONLY).count()
        finalDf1 = self._create_rank_temp_view(tmp1, "stock_cust_return_by_prd_tmp1", compute)
        finalDf2 = self._create_rank_temp_view(tmp2, "stock_cust_return_by_prd_tmp2", compute)
        dropPartitionSql = \
            "alter table {0}.{1} drop if exists  partition(busi_date= '{2}',compute='{3}')".format(
                save_db, save_table, enddate, compute)
        save_data(self.sparkSession, save_db, save_table, enddate,
                  finalDf1.union(finalDf2).repartition(30), partitonByName=["busi_date", "compute"],
                  dropPartitonSql=dropPartitionSql,
                  defaultDropPartition=dropParition)

    @func_logging(level='info')
    def _get_profit_percent(self, df, where):
        predf = df.where(where).groupby("trade_id").agg(F.sum('return').alias('all_return'))
        predf.persist(StorageLevel.DISK_ONLY).count()
        return predf

    @func_logging(level="info")
    def _create_agg_by_month(self, dfBase, enddate):
        dfBaseRdd = dfBase.rdd.mapPartitions(lambda ite: _travel_ite(ite, enddate, False), 100)
        if dfBaseRdd is None or dfBaseRdd.isEmpty():
            print "dfBaseRdd empty,after _travel_row2"
            return
        df = self.sparkSession.createDataFrame(dfBaseRdd)
        df.persist(StorageLevel.DISK_ONLY).count()
        return df

    @func_logging(level='info')
    def _create_prefinaldf_df(self, dfBase, enddate):
        dfBaseRdd = dfBase.rdd.mapPartitions(lambda ite: _travel_ite(ite, enddate), 100)
        if dfBaseRdd is None or dfBaseRdd.isEmpty():
            print "dfBaseRdd empty,after _travel_row2"
            return
        df = self.sparkSession.createDataFrame(dfBaseRdd)
        df.persist(StorageLevel.DISK_ONLY).count()
        return df

    def _create_rank_temp_view(self, df, table, compute):
        if self.logLevel != 'debug':
            df.createOrReplaceTempView(table)
        sql = """
              select a.*,{1} compute,
              row_number() over(partition by trade_id order by return_rate desc) return_rate_rank
              from (
                 select * ,row_number() over(partition by trade_id order by return desc) return_rank
                 from {0}
              ) a
              """
        df = self.sparkSession.sql(sql.format(table, compute))
        df.persist(StorageLevel.DISK_ONLY).count()
        if self.logLevel != 'debug':
            self.sparkSession.catalog.dropTempView(table)
        return df

    def _cal_return_ratio(self, df1, df2, whereCond=" return >=0 "):
        df2 = df2.withColumn("trade_id_a", df2.trade_id)
        cond = [df1.trade_id == df2.trade_id_a]
        tmp1 = df1.join(df2, cond, "inner")\
            .where(whereCond)\
            .select(df1["*"], F.when(df2.all_return != 0, F.col("return") / df2.all_return)
                    .otherwise(0).alias("return_ratio"))
        return tmp1
