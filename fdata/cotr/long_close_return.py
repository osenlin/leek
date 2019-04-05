# -*- coding: utf-8 -*-


from leek.fdata.cotr.compute_template import computeTemplate
from leek.fdata.cotr.util import sort_long_unclose_data, sort_long_close_data, sort_long_unclose_cal, get_timestamp, long_compute, long_trans_close, long_trans_unclose
from leek.common.util import get_date, save_data, setup_spark, full_outer_join

from pyspark.sql import functions as func
from pyspark.sql import Row
from pyspark import StorageLevel

import sys


def _init_trans(data):
    """
    """
    a = {}
    a["trade_id"] = data["trade_id"]
    a["secu_acc_id"] = data["secu_acc_id"]
    a["prd_no"] = data["prd_no"]
    a["busi_date"] = data["busi_date"]
    a["open_detail"] = [{
        "open_date": data["busi_date"],
        "open_timestamp": str(get_timestamp(data["busi_date"], 9)),
        "open_type": "init",
        "orig_trd_qty": str(data["qty"]),
        "orig_trd_amt": str(data["mkt_val"]),
        "trd_qty": str(data["qty"]),
        "trd_amt": str(data["mkt_val"]),
        "close_amt": str(0.0),
        "unclose_qty": str(data["qty"]),
        "unclose_amt": str(data["mkt_val"]),
        "weighted_term": str(1.0),
        "exception_label": str(0)}]
    return Row(**a)


def _compute(data, busi_date, yesterday):
    """
    """
    return Row(**long_compute(data, busi_date, yesterday))


class longCloseTradeReturn(computeTemplate):
    """
    """

    def __init__(self, spark):
        computeTemplate.__init__(self, spark)
        self.close_table = self.conf["long_close_trade_table"]
        self.unclose_table = self.conf["long_unclose_trade_table"]
        self.unclose_cal = self.conf["long_unclose_trade_cal"]
        self.asset_table = self.conf["asset_table"]
        self.cash_flow_table = self.conf["cash_flow_table"]

    def daily_compute(self, busi_date):
        """
        """
        spark = self.spark
        yesterday = get_date(
            self.date_order, self.order_date, busi_date, -1)
        unclose = spark.sql("select * from %s.%s where busi_date='%s'"
                            % (self.fdata, self.unclose_cal, yesterday))
        trd = spark.sql("select * from %s.%s where busi_date='%s'"
                        % (self.odata, self.cash_flow_table, busi_date))
        trd = trd.filter("trd_type='long_related' and prd_no!='0.0'")
        # trd_qty=0时，一定有amortize_label!=0 (==1)
        trd = trd.filter("trd_qty!=0 or amortize_label!=0")
        asset = spark.sql("select * from %s.%s where busi_date='%s'"
                          % (self.odata, self.asset_table, busi_date))
        asset = asset.filter("prd_no!='0.0'")
        asset = asset.select("trade_id", "secu_acc_id", "prd_no",
                             "qty", "mkt_val")
        trd = trd.withColumn(
            "now_trd", func.struct("timestamp", "trd_qty",
                                  "trd_cash_flow", "amortize_label"))
        trd = trd.groupBy("trade_id", "secu_acc_id", "prd_no") \
            .agg(func.collect_list("now_trd").alias("now_trd"))
        trd.persist(StorageLevel.DISK_ONLY)
        trd.count()
        df = full_outer_join(unclose, trd, ["trade_id", "secu_acc_id", "prd_no"])
        df = full_outer_join(df, asset, ["trade_id", "secu_acc_id", "prd_no"])
        df.persist(StorageLevel.DISK_ONLY)
        df.count()
        data = df.rdd.map(
            lambda x: _compute(x.asDict(recursive=True), busi_date, yesterday))
        data.persist(StorageLevel.DISK_ONLY)
        if data.count() > 0:
            self.save_close_data(data, busi_date,
                                 lambda t: int(t["unclose_qty"]) == 0,
                                 sort_long_close_data)
            self.save_unclose_data(data, busi_date,
                                   lambda t: int(t["unclose_qty"]) > 0,
                                   sort_long_unclose_data)
            self.save_unclose_cal(data, busi_date,
                                  lambda t: int(t["unclose_qty"]) > 0,
                                  sort_long_unclose_cal)

    def init_data(self, busi_date):
        self._init_data(busi_date)

    def _init_data(self, busi_date):
        """
        """
        data = self.get_init_data(busi_date)
        data = data.rdd.map(lambda x: _init_trans(x.asDict()))
        self.save_close_data(data, busi_date,
                             lambda t: int(t["unclose_qty"]) == 0,
                             sort_long_close_data)
        self.save_unclose_data(data, busi_date,
                               lambda t: int(t["unclose_qty"]) > 0,
                               sort_long_unclose_data)
        self.save_unclose_cal(data, busi_date,
                              lambda t: int(t["unclose_qty"]) > 0,
                              sort_long_unclose_cal)

    def get_init_data(self, busi_date):
        """
        """
        spark = self.spark
        command = "select * from %s.%s where busi_date='%s'" \
            % (self.odata, self.asset_table, busi_date)
        df = spark.sql(command)
        df = df.filter("prd_no!='0.0'")
        return df
        
    @staticmethod
    def _trans_close(data):
        """
        """
        for item in data["open_detail"]:
            a = long_trans_close(data, item)
            yield Row(**a)

    @staticmethod
    def _trans_unclose(data):
        """
        """
        for item in data["open_detail"]:
            a = long_trans_unclose(data, item)
            yield Row(**a)


def _run(mode, load_date):
    """
    """
    spark = setup_spark({})
    long_close = longCloseTradeReturn(spark)
    if mode == "init":
        long_close.init_data(load_date)
    else:
        long_close.daily_compute(load_date)


if __name__ == "__main__":
    # 根据传入的日期，更新数据
    if len(sys.argv) == 3:
        _, mode, load_date = sys.argv
        _run(mode, load_date)
    else:
        pass
