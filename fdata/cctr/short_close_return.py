# -*- coding: utf-8 -*-


from leek.fdata.cctr.compute_template import computeTemplate
from leek.fdata.cctr.util import _init_trans, days_between, trd_init_trans, \
    get_last_close_item, alltype2str, get_timestamp, flat_data
from leek.fdata.cctr.util import update_open_detail_yesterday_wt, \
    update_open_detail_current_day_wt
from leek.common.util import get_date, full_outer_join

from pyspark.sql import functions as func
from pyspark.sql import Row
from pyspark import StorageLevel
import time


def _compute(data, busi_date, yesterday):
    """
    """
    return Row(**short_compute(data, busi_date, yesterday))


def amortize_short_trade(open_detail, open_ind, trd):
    """
    """
    _qty_sum = sum([int(i["unclose_qty"]) for i in open_detail[open_ind:]])
    for item in open_detail[open_ind:]:
        ratio = float(item["unclose_qty"]) / _qty_sum
        item["trd_qty"] = str(
            int(item["trd_qty"]) + int(round(ratio * trd["trd_qty"])))
        item["unclose_qty"] = str(
            int(item["unclose_qty"]) + int(round(ratio * trd["trd_qty"])))
        item['gain_return'] = str(
            float(item['gain_return']) + ratio * trd["trd_cash_flow"])
        if trd["trd_cash_flow"] > 0:
            item["close_amt"] = str(
                float(item["close_amt"]) + ratio * trd["trd_cash_flow"])
        else:
            item["trd_amt"] = str(
                float(item["trd_amt"]) + ratio * trd["trd_cash_flow"])
    return open_detail


def compute_short_trade(pos, trd, open_ind, trd_ind, close_item, busi_date):
    """
    """
    # 注意：trd["trd_qty"]是一个正数
    dff_days = days_between(busi_date, pos['open_date'])
    pre_unclose_inv = float(pos["unclose_inv"])
    pre_unclose_qty = long(pos["unclose_qty"])
    if int(pos["unclose_qty"]) + trd["trd_qty"] <= 0:
        pos["unclose_qty"] = str(pre_unclose_qty + trd["trd_qty"])
        pos["close_amt"] = str(float(pos["close_amt"]) + trd["trd_cash_flow"])
        ratio = float(pre_unclose_qty + trd["trd_qty"]) / pre_unclose_qty
        pos["unclose_amt"] = str(ratio * float(pos["unclose_amt"]))
        trd_ind += 1
        if int(pos["unclose_qty"]) == 0:
            open_ind += 1
        pos["unclose_inv"] = ratio * pre_unclose_inv
        # 记录卖出收益
        close_item['open_date'] = pos['open_date'] \
            if close_item['open_date'] == "-1" else close_item['open_date']
        close_item["close_inv"] += float(
            trd["trd_qty"]) / pre_unclose_qty * pre_unclose_inv
        close_item['close_qty'] += long(trd["trd_qty"])
        close_item['close_amt'] += trd["trd_cash_flow"] + float(
            pos["gain_return"]) * - float(trd["trd_qty"]) / pre_unclose_qty
        close_item['close_timestamp'] = trd['timestamp']
        close_item['close_date'] = busi_date
        close_item['unclose_qty'] = 0
        close_item['unclose_amt'] = 0.
        close_amt = close_item["close_amt"]
        if close_amt != 0:
            close_item['return'] = float(close_item['close_inv']) - close_amt
            close_item['return_rate'] = close_item['return'] / close_amt
        else:
            close_item['return'] = 0.0
            close_item['return_rate'] = 0.0
        close_item['exception_label'] = close_item['exception_label'] | int(
            pos['exception_label'])
        close_item["weighted_term"] += abs(
            float(pos["weighted_term"]) * trd["trd_qty"] / close_item[
                'close_qty'])
    else:
        pos["close_amt"] = \
            str(float(pos["close_amt"]) + trd["trd_cash_flow"] * (
                -1 * pre_unclose_qty) / trd["trd_qty"])
        trd["trd_cash_flow"] *= 1 + float(pre_unclose_qty) / trd["trd_qty"]
        trd["trd_qty"] += pre_unclose_qty
        pos["unclose_qty"] = str(0)
        pos["unclose_amt"] = str(0.0)
        pos["unclose_inv"] = str(0.)
        close_item['open_date'] = pos['open_date'] \
            if close_item['open_date'] == "-1" else close_item['open_date']
        close_item["close_inv"] += -pre_unclose_inv
        close_item['close_qty'] += -pre_unclose_qty
        close_item['close_amt'] += \
            trd["trd_cash_flow"] * (-1 * pre_unclose_qty) / trd["trd_qty"] \
            + float(pos["gain_return"])
        close_item['unclose_qty'] = trd["trd_qty"]
        close_item['unclose_amt'] = trd["trd_cash_flow"]
        close_item['weighted_term'] += abs(
            float(pos["weighted_term"]) * pre_unclose_qty / (
                trd["trd_qty"] + close_item['close_qty']))
        close_item['exception_label'] = close_item['exception_label'] | int(
            pos['exception_label'])
        open_ind += 1
    close_item['close_type'] = 'trade'
    close_item['open_qty'] = close_item['close_qty']
    close_item['open_amt'] = close_item['close_inv']
    return pos, trd, open_ind, trd_ind


def short_compute(data, busi_date, yesterday):
    """
    """
    tol_bias = 1
    a = {}
    a["trade_id"] = data["trade_id"]
    a["secu_acc_id"] = data["secu_acc_id"]
    a["prd_no"] = data["prd_no"]
    a["busi_date"] = busi_date
    open_detail = data["open_detail"] if data["open_detail"] is not None else []
    # 更新持仓周期
    update_open_detail_yesterday_wt(open_detail, busi_date, yesterday)
    close_detail = []
    now_trd = data["now_trd"] if data["now_trd"] is not None else []
    now_trd = sorted(now_trd, key=lambda x: (x["timestamp"], x["trd_qty"]))
    open_ind = 0
    trd_ind = 0
    liab_qty = data["liab_qty"] if data["liab_qty"] is not None else 0
    mkt_val = data["mkt_val"] if data["mkt_val"] is not None else 0.0
    while trd_ind < len(now_trd):
        trd = now_trd[trd_ind]
        if trd["amortize_label"] == 1:
            open_detail = amortize_short_trade(open_detail, open_ind, trd)
            trd_ind += 1
        elif trd["trd_qty"] < 0:
            open_detail.append(trd_init_trans(trd, busi_date))
            trd_ind += 1
        elif trd["trd_qty"] > 0:
            close_item = get_last_close_item(close_detail, trd, busi_date)
            if open_ind < len(open_detail):
                pos = open_detail[open_ind]
                pos, trd, open_ind, trd_ind = compute_short_trade(pos, trd,
                                                                  open_ind,
                                                                  trd_ind,
                                                                  close_item,
                                                                  busi_date)
            else:
                # 数据出错了，忽略买入行为
                trd_ind += 1
    for pos in reversed(open_detail[open_ind:]):
        if int(pos["unclose_qty"]) >= liab_qty - tol_bias:
            if abs(int(pos["unclose_qty"]) - liab_qty) <= tol_bias:
                current_qty = liab_qty
            else:
                current_qty = int(pos["unclose_qty"])
            pos["unclose_qty"] = str(current_qty)
            if liab_qty != 0:
                pos["unclose_amt"] = str(
                    float(current_qty) / liab_qty * mkt_val)
                mkt_val *= (liab_qty - float(current_qty)) / liab_qty
                liab_qty -= current_qty
            else:
                pos["unclose_amt"] = str(0.0)
                mkt_val = 0.0
                liab_qty = 0
        else:
            pos["exception_label"] = str(1)
            # 异常处理一下
            pre_unclose_qty = float(pos["unclose_qty"])
            pre_unclose_amt = float(pos["unclose_amt"])
            ratio = (int(pos["unclose_qty"]) - liab_qty) / pre_unclose_qty
            pos["close_amt"] = str(
                float(pos["close_amt"]) - pre_unclose_amt * ratio)
            pos["unclose_qty"] = str(liab_qty)
            pos["unclose_amt"] = str(mkt_val)
            pos["unclose_inv"] = float(pos["unclose_inv"])
            # 补齐买入的行为
            c_a_item = {}
            c_a_item['open_date'] = pos['open_date']
            c_a_item['close_inv'] = -float(pos['unclose_inv']) * ratio
            c_a_item['open_amt'] = c_a_item['close_inv']
            c_a_item['close_qty'] = liab_qty - pre_unclose_qty
            c_a_item['open_qty'] = c_a_item['close_qty']
            c_a_item['close_amt'] = - pre_unclose_amt * ratio + mkt_val - (
                1 - ratio) * pre_unclose_amt + float(pos["gain_return"])
            c_a_item['unclose_qty'] = 0
            c_a_item['unclose_amt'] = 0.
            c_a_item['exception_label'] = 1
            c_a_item['return'] = c_a_item['close_inv'] - c_a_item["close_amt"]
            c_a_item['return_rate'] = c_a_item['return'] / c_a_item[
                "close_amt"] if c_a_item["close_amt"] != 0 else 0.
            c_a_item['close_date'] = busi_date
            c_a_item['close_timestamp'] = int(round(time.time() * 1000))
            c_a_item['exception_label'] = 1
            c_a_item["weighted_term"] = abs(float(pos["weighted_term"]) * ratio)
            c_a_item['close_type'] = 'trade_exception'
            close_detail.append(c_a_item)
            liab_qty = 0
            mkt_val = 0.0
    # 持仓异常处理
    if liab_qty < -1 * tol_bias:
        open_detail.append(
            dict(open_date=busi_date,
                 open_timestamp=str(get_timestamp(busi_date, 15) + 1),
                 open_type="trade_exception",
                 orig_trd_qty=str(liab_qty),
                 orig_trd_amt=str(mkt_val),
                 trd_qty=str(liab_qty),
                 trd_amt=str(mkt_val),
                 close_amt=str(0.0),
                 unclose_inv=str(mkt_val),
                 gain_return=str(0.),
                 unclose_qty=str(liab_qty),
                 unclose_amt=str(mkt_val),
                 weighted_term=str(0.0),
                 exception_label=str(1)))
    a["open_detail"] = open_detail
    a['close_detail'] = map(alltype2str, close_detail)
    # 更新持仓周期
    update_open_detail_current_day_wt(open_detail)
    return a


class shortCloseTradeReturn(computeTemplate):
    """
    """

    def __init__(self, spark):
        computeTemplate.__init__(self, spark)
        self.close_table = self.conf["short_c_close_trade_table"]
        self.unclose_table = self.conf["short_c_unclose_trade_table"]
        self.unclose_cal = self.conf["short_c_unclose_trade_cal"]
        self.debt_table = self.conf["debt_table"]
        self.cash_flow_table = self.conf["cash_flow_table"]

    def init_data(self, busi_date):
        self._init_data(busi_date)

    def _init_data(self, busi_date):
        """
        """
        spark = self.spark
        command = "select * from %s.%s where busi_date='%s'" % (
            self.odata, self.debt_table, busi_date)
        data = spark.sql(command).filter("prd_no!='0.0'")
        data = data.rdd.map(lambda x: _init_trans(x.asDict(), "short"))
        self.save_trade_unclose_data(data, spark, busi_date, lambda t: int(t["unclose_qty"]) < 0)

    def daily_compute(self, busi_date):
        """
        """
        spark = self.spark
        yesterday = get_date(self.date_order, self.order_date, busi_date, -1)
        unclose = spark.sql("select * from %s.%s where busi_date='%s'" % (
            self.fdata, self.unclose_cal, yesterday))
        trd = spark.sql("select * from %s.%s where busi_date='%s'" % (
            self.odata, self.cash_flow_table, busi_date))\
            .filter("trd_type='short_related' and prd_no!='0.0'")\
            .filter("trd_qty!=0 or amortize_label!=0")\
            .withColumn("now_trd", func.struct("timestamp", "trd_qty",
                                               "trd_cash_flow",
                                               "amortize_label"))\
            .groupBy("trade_id", "secu_acc_id", "prd_no").\
            agg(func.collect_list("now_trd").alias("now_trd")).\
            persist(StorageLevel.DISK_ONLY)
        trd.count()
        debt = spark.sql("select * from %s.%s where busi_date='%s'" % (
            self.odata, self.debt_table, busi_date))\
            .filter("prd_no!='0.0'")\
            .select("trade_id", "secu_acc_id", "prd_no", "liab_qty", "mkt_val")
        df = full_outer_join(unclose, trd,
                             ["trade_id", "secu_acc_id", "prd_no"])
        df = full_outer_join(df, debt, ["trade_id", "secu_acc_id", "prd_no"])
        df.persist(StorageLevel.DISK_ONLY).count()
        data = df.rdd.map(
            lambda x: _compute(x.asDict(recursive=True), busi_date, yesterday))
        if data.count() > 0:
            data.persist(StorageLevel.DISK_ONLY).count()
            self.save_trade_close_data(data, spark, busi_date)
            self.save_trade_unclose_data(data, spark, busi_date,
                                         lambda t: int(t["unclose_qty"]) < 0)
        else:
            print '没有做空开仓交易的数据'

    @staticmethod
    def _trans_unclose(data):
        def short_trans_unclose(data, item):
            a = flat_data(data, item)
            a["unclose_qty"] = int(item["unclose_qty"])
            a["unclose_amt"] = float(item["unclose_amt"])
            a["close_qty"] = a["trd_qty"] - a["unclose_qty"]
            if a["close_amt"] - a["unclose_amt"] == 0:
                a["return"] = 0.0
                a["return_rate"] = 0.0
            else:
                a["return"] = -a["trd_amt"] - (a["close_amt"] - a["unclose_amt"])
                a["return_rate"] = a["return"] / (a["close_amt"] - a["unclose_amt"])
            return a

        for item in data["open_detail"]:
            a = short_trans_unclose(data, item)
            yield Row(**a)