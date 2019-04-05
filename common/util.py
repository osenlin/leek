# -*- coding: utf-8 -*-


import calendar
from datetime import datetime, timedelta


def get_config():
    """
    """
    config = {
        "tool_database": "clean_tool",
        "original_database": "odata",
        "feature_database": "fdata",
        "aggregation_database": "adata",
        # end - database
        "calendar_table": "trading_day",
        # begin - odata table
        "asset_table": "stock_asset_holding",
        "debt_table": "stock_debt_holding",
        "cash_flow_table": "stock_cash_flow_detail",
        "stock_description_table": "stock_description",
        "index_description_table": "index_description",
        "stock_price_table": "stock_price",
        "index_price_table": "index_price",
        # end - odata table
        "hk_asset_table": "hk_stock_asset_holding",
        "hk_debt_table": "hk_stock_debt_holding",
        "hk_cash_flow_table": "hk_stock_cash_flow_detail",
        "hk_stock_cash_flow_merge_table": "hk_stock_cash_flow",
        "hk_check_data_table": "hk_stock_daily_check_data",
        "hk_check_data_exception_table": "hk_stock_daily_check_exception",
        "hk_stock_daily_check_report": "hk_stock_daily_check_report",
        # begin - fdata table
        "stock_cash_flow_merge_table": "stock_cash_flow",
        "check_data_table": "stock_daily_check_data",
        "check_data_exception_table": "stock_daily_check_exception",
        "stock_return_rate_range_table": "stock_return_rate_range",
        "stock_daily_check_report": "stock_daily_check_report",
        "long_close_table": "stock_close_prd_long_data",
        "long_unclose_table": "stock_unclose_prd_long_data",
        "short_close_table": "stock_close_prd_short_data",
        "short_unclose_table": "stock_unclose_prd_short_data",
        "long_close_trade_table": "stock_close_o_trade_long_data",
        "long_unclose_trade_table": "stock_unclose_o_trade_long_data",
        "long_unclose_trade_cal": "stock_unclose_o_trade_long_cal",
        "short_close_trade_table": "stock_close_o_trade_short_data",
        "short_unclose_trade_table": "stock_unclose_o_trade_short_data",
        "short_unclose_trade_cal": "stock_unclose_o_trade_short_cal",
        "long_c_close_trade_table": "stock_close_c_trade_long_data",
        "long_c_unclose_trade_table": "stock_unclose_c_trade_long_data",
        "long_c_unclose_trade_cal": "stock_unclose_c_trade_long_cal",
        "short_c_close_trade_table": "stock_close_c_trade_short_data",
        "short_c_unclose_trade_table": "stock_unclose_c_trade_short_data",
        "short_c_unclose_trade_cal": "stock_unclose_c_trade_short_cal",
        "stock_shortterm_return_rate_table": "stock_shortterm_return_rate",
        "index_shortterm_return_rate_table": "index_shortterm_return_rate",
        "stock_close_tradebyday_long_data_table": "stock_close_tradebyday_long_data",
        "stock_close_tradebyday_short_data_table": "stock_close_tradebyday_short_data",
        "stock_unclose_tradebyday_long_data_table": "stock_unclose_tradebyday_long_data",
        "stock_unclose_tradebyday_short_data_table": "stock_unclose_tradebyday_short_data",
        "stock_unclose_tradebyday_long_cal_table": "stock_unclose_tradebyday_long_cal",
        "stock_unclose_tradebyday_short_cal_table": "stock_unclose_tradebyday_short_cal",
        # end - fdata table
        # begin - adata table
        "stock_cust_daily_holding_table": "stock_cust_daily_holding",
        "stock_cust_daily_holding_ssid_table": "stock_cust_daily_holding_ssid",
        "stock_cust_daily_return_table": "stock_cust_daily_return",
        "stock_cust_daily_return_ssid_table": "stock_cust_daily_return_ssid",
        "asset_cust_daily_return_table": "asset_cust_daily_return",
        "stock_return_by_month_table": "stock_return_by_month",
        "stock_cust_return_by_prd_table": "stock_cust_return_by_prd",
        "stock_cust_return_by_prd_ind_table": "stock_cust_return_by_prd_ind",
        "stock_cust_return_by_prd_temp_table": "stock_cust_return_by_prd_by_month",
        "stock_cust_return_by_prd_ind_temp_table": "stock_cust_return_by_prd_ind_by_month",
        "stock_cust_investment_ability_table": "stock_cust_investment_ability",
        "stock_cust_trd_quant_table": "stock_cust_trd_quant",
        "stock_cust_investment_rank_score_table": "stock_cust_investment_rank_score",
        "stock_cust_pl_analysis_table": "stock_cust_pl_analysis",
        "stock_cust_diagnosis_sell_time_table": "stock_cust_diagnosis_sell_time",
        "stock_cust_diagnosis_sell_ind_table": "stock_cust_diagnosis_sell_ind",
        "stock_cust_diagnosis_sell_prd_table": "stock_cust_diagnosis_sell_prd",
        "stock_cust_diagnosis_buy_time_table": "stock_cust_diagnosis_buy_time",
        "stock_cust_diagnosis_buy_ind_table": "stock_cust_diagnosis_buy_ind",
        "stock_cust_diagnosis_buy_prd_table": "stock_cust_diagnosis_buy_prd",
        "stock_close_tradebyday_by_month_table": "stock_close_tradebyday_by_month",
        "stock_tradebyday_data_table": "stock_tradebyday_data",
        "stock_ac_check_exception_data_table": "stock_ac_check_exception_data",
        "stock_ac_check_exception_report_table": "stock_ac_check_exception_report",
        "stock_dr_check_exception_data_table": "stock_dr_check_exception_data",
        "stock_dr_check_exception_report_table": "stock_dr_check_exception_report",
        "asset_dr_check_exception_data_table": "asset_dr_check_exception_data",
        "asset_dr_check_exception_report_table": "asset_dr_check_exception_report",
        "stock_prd_ind_check_exception_data_table": "stock_prd_ind_check_exception_data",
        "stock_prd_ind_check_exception_report_table": "stock_prd_ind_check_exception_report",
        "stock_prd_mon_check_exception_data_table": "stock_prd_mon_check_exception_data",
        "stock_prd_mon_check_exception_report_table": "stock_prd_mon_check_exception_report",
        "stock_trdrr_check_exception_data_table": "stock_trdrr_check_exception_data",
        "stock_oc_check_exception_data_table": "stock_oc_check_exception_data",
        "stock_oc_check_exception_report_table": "stock_oc_check_exception_report",
        "stock_otbd_check_exception_data_table": "stock_otbd_check_exception_data",
        "stock_otbd_check_exception_report_table": "stock_otbd_check_exception_report",
        "stock_odc_check_exception_data_table": "stock_odc_check_exception_data",
        "stock_odc_check_exception_report_table": "stock_odc_check_exception_report",
        "stock_oprd_check_exception_data_table": "stock_oprd_check_exception_data",
        "stock_oprd_check_exception_report_table": "stock_oprd_check_exception_report",
        "stock_inv_pl_check_exception_data_table": "stock_inv_pl_check_exception_data",
        "stock_inv_pl_check_exception_report_table": "stock_inv_pl_check_exception_report",
        "stock_cdc_check_exception_data_table": "stock_cdc_check_exception_data",
        "stock_cdc_check_exception_report_table": "stock_cdc_check_exception_report"
        # end -adata table
    }
    return config


def get_natural_date(d1, diff_days, format="%Y-%m-%d"):
    if isinstance(d1, str):
        realdate = datetime.strptime(d1, format)
    elif isinstance(d1, datetime):
        realdate = datetime
    else:
        raise Exception("d1 isn't str or datetime")
    return realdate + timedelta(days=diff_days)


def get_trading_day(spark, database, table):
    """
    """

    def fold_dict(a, b):
        a.update(b)
        return a

    command = "select * from %s.%s" % (database, table)
    df = spark.sql(command)
    date_order = df.rdd.map(lambda x: {x.busi_date: x.rlt_rank}).reduce(fold_dict)
    order_date = dict([date_order[i], i] for i in date_order)
    date_order = spark._sc.broadcast(date_order)
    order_date = spark._sc.broadcast(order_date)
    return date_order, order_date


def get_date(date_order, order_date, busi_date, diff_num):
    """
    """
    date_rank = date_order.value.get(busi_date) + diff_num
    return order_date.value.get(date_rank)


def days_between(d1, d2):
    """
    """
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


def get_date_by_month_diff(d1, month_diff):
    realDate = datetime.strptime(d1, "%Y-%m-%d")
    month = realDate.month
    year = realDate.year
    if month - month_diff <= 0:
        year = year - 1
        month = 12 + month - month_diff
    else:
        month = month - month_diff
    return datetime(year, month, 1)


def full_outer_join(df, other_df, column_name, logic='and'):
    """
    """
    sql_context = df.sql_ctx
    # Register temp table for ETL
    df.createOrReplaceTempView("tmp1")
    other_df.createOrReplaceTempView("tmp2")
    # Construct column name
    name1 = [i[0] for i in df.dtypes if i[0] not in column_name]
    name2 = [i[0] for i in other_df.dtypes if i[0] not in column_name]
    union = [i for i in name1 if i in name2]
    # assert len(union) == 0, "%s exist in two table, but not in join condition" % (union)
    _name1 = [i for i in name1 if i not in union]
    _name2 = [i for i in name2 if i not in union]
    name_1 = ["%s.%s as %s" % ("tmp1", i, i) for i in _name1]
    name_2 = ["%s.%s as %s" % ("tmp2", i, i) for i in _name2]
    name_3 = ["if(tmp1.%s is null,tmp2.%s,tmp1.%s) as %s" % (i, i, i, i) for i in column_name]
    name_4 = ["if(tmp1.%s is null,tmp2.%s,tmp1.%s) as %s" % (i, i, i, i) for i in union]
    condition = (" %s " % logic).join(["tmp1.%s=tmp2.%s" % (i, i) for i in column_name])
    select_item = ",".join(name_1 + name_4 + name_3 + name_2)
    sql_command = "select %s from tmp1 full outer join tmp2 on (%s)" % (select_item, condition)
    # print sqlCommand
    return sql_context.sql(sql_command)


def save_data(spark, database, table, busi_date, data, partitonByName="busi_date",
              dropPartitonSql=None, defaultDropPartition=True):
    """
    将计算结果保存到HIVE
    """
    spark.sql("create database if not exists %s" % database)
    spark.sql("use %s" % database)
    tables = [i.name for i in spark.catalog.listTables()]
    if table in tables:
        _df = spark.sql("select * from %s.%s" % (database, table))
        # 保证数据的schema和已有HIVE表的schema一致
        data = spark.createDataFrame(data.select(_df.columns).rdd, _df.schema)
        data = data.repartition(spark._sc.defaultParallelism)
        # 如果分区上已有数据，则覆写
        if defaultDropPartition:
            if not dropPartitonSql:
                dropsql = "alter table %s.%s drop if exists partition(busi_date='%s')" % (
                    database, table, busi_date)
                spark.sql(dropsql)
            else:
                spark.sql(dropPartitonSql)
        data.write.saveAsTable("%s.%s" % (database, table), format="orc",
                               partitionBy=partitonByName, mode="append")
    else:
        data = data.repartition(spark._sc.defaultParallelism)
        data.write.saveAsTable("%s.%s" % (database, table), format="orc",
                               partitionBy=partitonByName)
    spark.sql("refresh table %s.%s" % (database, table))


def setup_spark(cluster_conf):
    """
    Setup spark cluster

    Parameters
    ----------
    clusterConf: dict
        The additional configuration of PyEsConf
    """
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    conf = SparkConf()
    for key, value in cluster_conf.iteritems():
        conf.set(key, value)
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    return spark


def _test_setup_spark():
    spark = setup_spark({"1": "2"})
    rdd = spark._sc.parallelize(range(10))
    print rdd.count()


if __name__ == "__main__":
    _test_setup_spark()
