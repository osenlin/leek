# -*- coding: utf-8 -*-


from leek.fdata.csr.compute_template import computeTemplate
from leek.fdata.csr.util import sort_short_close_data, sort_short_unclose_data, short_compute
from leek.common.util import get_date, save_data, setup_spark, full_outer_join, days_between

from pyspark.sql import functions as func
from pyspark.sql import Row
from pyspark import StorageLevel
from pyspark.sql.types import DoubleType

import sys


def _init_trans(data, busi_date):
    """
    """
    a = {}
    a["trade_id"] = data["trade_id"]
    a["secu_acc_id"] = data["secu_acc_id"]
    a["prd_no"] = data["prd_no"]
    a["busi_date"] = data["busi_date"]
    a["open_date"] = data["busi_date"]
    a["holding_term"] = 1L
    a["return"] = 0.0
    a["return_rate"] = 0.0
    a["total_in"] = data["mkt_val"]
    a["total_out"] = 0.0
    a["remain_liab_qty"] = data["liab_qty"]
    a["remain_val"] = data["mkt_val"]
    a["trd_detail"] = [{
        "trd_date": data["busi_date"],
        "trd_qty": str(data["liab_qty"]),
        "trd_amt": str(data["mkt_val"]),
        "return": str(0.0)}]
    a["exception_label"] = 0L
    return Row(**a)


class shortCloseReturn(computeTemplate):
    """
    """

    def __init__(self, spark):
        computeTemplate.__init__(self, spark)
        self.close_table = self.conf["short_close_table"]
        self.unclose_table = self.conf["short_unclose_table"]
        self.cash_flow_table = self.conf["cash_flow_table"]
        self.debt_table = self.conf["debt_table"]

    def init_data(self, busi_date):
        """
        """
        self._init_data(busi_date)

    def daily_compute(self, busi_date):
        """
        """
        spark = self.spark
        yesterday = get_date(
            self.date_order, self.order_date, busi_date, -1)
        unclose = spark.sql("select * from %s.%s where busi_date='%s'"
                            % (self.fdata, self.unclose_table, yesterday))
        trd = spark.sql("select * from %s.%s where busi_date='%s'"
                        % (self.odata, self.cash_flow_table, busi_date))
        trd = trd.filter("trd_type='short_related' and prd_no!='0.0'")
        trd = trd.withColumn(
            "now_trd", func.struct("trd_qty", "trd_cash_flow", "timestamp"))
        trd = trd.groupBy("trade_id", "secu_acc_id", "prd_no") \
            .agg(func.collect_list("now_trd").alias("now_trd"))
        trd.persist(StorageLevel.DISK_ONLY)
        trd.count()
        debt = spark.sql("select * from %s.%s where busi_date='%s'"
                          % (self.odata, self.debt_table, busi_date))
        debt = debt.select("trade_id", "secu_acc_id", "prd_no", "liab_qty", "mkt_val")
        debt = debt.filter("prd_no!='0.0'")
        df = full_outer_join(unclose, trd, ["trade_id", "secu_acc_id", "prd_no"])
        df = full_outer_join(df, debt, ["trade_id", "secu_acc_id", "prd_no"])
        df.persist(StorageLevel.DISK_ONLY)
        df.count()
        data = df.rdd.map(lambda x: Row(**short_compute(x.asDict(recursive=True), busi_date)))
        if data.count() >0 :
            data = data.toDF()
            close = data.filter("remain_liab_qty = 0")
            close = close.withColumn("close_date", close.busi_date)
            self.check_data(close)
            unclose = data.filter("remain_liab_qty != 0")
            self.save_close_data(close, busi_date)
            self.save_unclose_data(unclose, busi_date)
        else:
            print "清仓股票收益做空没有数据输出"

    def check_data(self, close):
        """
        """
        def sum_return(data):
            x = 0.0
            for i in data:
                x += float(i["return"])
            return x

        sum_return_udf = func.udf(sum_return, DoubleType())
        data = close.withColumn("sum_return", sum_return_udf(close.trd_detail))
        print "***************************************************"
        print "The close short trade error num is %s, which should be 0" \
            % data.filter("abs(return-sum_return) > 1 and exception_label=0").count()
        data.filter("abs(return-sum_return) > 1 and exception_label=0").show()

    def _init_data(self, busi_date):
        """
        """
        data = self.get_init_data(busi_date)
        data = data.rdd.map(lambda x: _init_trans(x.asDict(), busi_date))
        if data.count()> 0:
            df = data.toDF()
            close = df.filter("remain_liab_qty = 0")
            close = close.withColumn("close_date", close.busi_date)
            unclose = df.filter("remain_liab_qty != 0")
            self.save_close_data(close, busi_date)
            self.save_unclose_data(unclose, busi_date)

    def get_init_data(self, busi_date):
        """
        """
        spark = self.spark
        command = "select * from %s.%s where busi_date='%s'" \
            % (self.odata, self.debt_table, busi_date)
        df = spark.sql(command)
        df = df.filter("prd_no!='0.0'")
        return df

    def save_close_data(self, data, busi_date):
        """
        """
        data = sort_short_close_data(data)
        save_data(self.spark, self.fdata, self.close_table, busi_date, data)

    def save_unclose_data(self, data, busi_date):
        """
        """
        data = sort_short_unclose_data(data)
        save_data(self.spark, self.fdata, self.unclose_table, busi_date, data)
