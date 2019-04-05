# -*- coding: utf-8 -*-
from leek.fdata.cctr.compute_template import computeTemplate
from leek.fdata.cctr.util import get_timestamp, \
    update_open_detail_current_day_wt, _init_trans, flat_data
from leek.fdata.cctr.util import update_open_detail_yesterday_wt, \
    trd_init_trans, get_last_close_item, alltype2str
from leek.common.util import get_date, full_outer_join
import time
from pyspark.sql import functions as func
from pyspark.sql import Row
from pyspark import StorageLevel


def _compute(data, busi_date, yesterday):
    """
    """
    return Row(**long_compute(data, busi_date, yesterday))


def amortize_long_trade(open_detail, open_ind, trd):
    _qty_sum = sum([int(i["unclose_qty"]) for i in open_detail[open_ind:]])
    for item in open_detail[open_ind:]:
        ratio = float(item["unclose_qty"]) / _qty_sum
        item["trd_qty"] = str(
            int(item["trd_qty"]) + int(round(ratio * trd["trd_qty"])))
        item["unclose_qty"] = str(
            int(item["unclose_qty"]) + int(round(ratio * trd["trd_qty"])))
        if trd["trd_cash_flow"] >= 0:
            item["trd_amt"] = str(
                float(item["trd_amt"]) + ratio * trd["trd_cash_flow"])
            item["unclose_inv"] = str(
                float(item["unclose_inv"]) + ratio * trd["trd_cash_flow"])
        else:
            item["close_amt"] = str(
                float(item["close_amt"]) - ratio * trd["trd_cash_flow"])
            item['gain_return'] = str(
                -(float(item["gain_return"]) + ratio * trd["trd_cash_flow"]))


def compute_long_trade(pos, trd, open_ind, trd_ind, close_item, busi_date):
    """
    """
    # 注意：trd["trd_qty"]是一个负数
    pre_close_amt = float(pos["close_amt"])
    pre_unclose_qty = int(pos["unclose_qty"])
    if int(pos["unclose_qty"]) + trd["trd_qty"] >= 0:
        '''
        卖出数量小于之前某单笔买入数量
        '''
        pre_unclose_inv = float(pos["unclose_inv"])
        pos["unclose_qty"] = str(pre_unclose_qty + trd["trd_qty"])
        pos["close_amt"] = str(pre_close_amt - trd["trd_cash_flow"])
        ratio = float(pre_unclose_qty + trd["trd_qty"]) / pre_unclose_qty
        pos["unclose_amt"] = str(ratio * float(pos["unclose_amt"]))
        pos["unclose_inv"] = ratio * pre_unclose_inv
        # 记录卖出收益
        close_item['open_date'] = pos['open_date'] \
            if close_item['open_date'] == "-1" else close_item['open_date']
        close_item["close_inv"] += float(
            -trd["trd_qty"]) / pre_unclose_qty * pre_unclose_inv
        close_item['close_qty'] += -long(trd["trd_qty"])
        close_item['close_amt'] += -float(trd['trd_cash_flow']) + float(
            pos['gain_return']) * (1 - ratio)
        close_item['close_timestamp'] = long(trd['timestamp'])
        close_item['close_date'] = busi_date
        close_item['unclose_qty'] = 0
        close_item['unclose_amt'] = 0.
        close_inv = float(close_item['close_inv'])
        if close_inv != 0 :
            close_item['return'] = close_item["close_amt"] - close_inv
            close_item['return_rate'] = close_item['return'] / close_inv
        else:
            close_item['return'] = 0.0
            close_item['return_rate'] = 0.0
        close_item['exception_label'] = close_item['exception_label'] | int(
            pos['exception_label'])
        close_item["weighted_term"] += abs(
            float(pos["weighted_term"]) * -trd["trd_qty"] / close_item[
                'close_qty'])
        trd_ind += 1
        if int(pos["unclose_qty"]) == 0:
            # 计算处理下一条之前买入的数据
            open_ind += 1
        pos['gain_return'] = str(float(pos['gain_return']) * ratio)
    else:
        '''
        单笔卖出数量大于单笔买入数量
        '''
        pre_unclose_qty = long(pos["unclose_qty"])
        pre_trd_cash_flow = trd["trd_cash_flow"]
        pre_unclose_inv = float(pos["unclose_inv"])
        ratio = float(pre_unclose_qty) / trd["trd_qty"]
        pos["close_amt"] = str(pre_close_amt + pre_trd_cash_flow * ratio)
        trd["trd_cash_flow"] = pre_trd_cash_flow * (1 + ratio)
        trd["trd_qty"] += pre_unclose_qty
        pos["unclose_qty"] = str(0)
        pos["unclose_amt"] = str(0.0)
        pos["unclose_inv"] = str(0.)
        # 中间结果先不算收益
        close_item['open_date'] = pos['open_date'] \
            if close_item['open_date'] == "-1" else close_item['open_date']
        close_item["close_inv"] += pre_unclose_inv
        close_item['close_qty'] += pre_unclose_qty
        close_item['close_amt'] += pre_trd_cash_flow * ratio + float(
            pos['gain_return'])
        close_item['unclose_qty'] = -trd["trd_qty"]
        close_item['unclose_amt'] = -trd["trd_cash_flow"]
        all_trd_qy = -trd["trd_qty"] + close_item['close_qty']
        close_item['weighted_term'] += abs(
            float(pos["weighted_term"]) * pre_unclose_qty / all_trd_qy)
        close_item['exception_label'] = close_item['exception_label'] | int(
            pos['exception_label'])
        open_ind += 1
    close_item['close_type'] = 'trade'
    close_item['open_qty'] = close_item['close_qty']
    close_item['open_amt'] = close_item['close_inv']
    return pos, trd, open_ind, trd_ind


def _except_long_qty(pos, close_detail, qty, mkt_val, busi_date):
    # 对最后一笔交易数据进行修正
    pre_unclose_qty = long(pos["unclose_qty"])
    pre_unclose_amt = float(pos["unclose_amt"])
    pre_close_amt = float(pos['close_amt'])
    pos["exception_label"] = str(1)
    ratio = float(pre_unclose_qty - qty) / pre_unclose_qty
    pos["close_amt"] = str(float(pre_close_amt) + pre_unclose_amt * ratio)
    pos["unclose_qty"] = str(qty)
    pos["unclose_amt"] = str(mkt_val)
    pos["unclose_inv"] = float(pos["unclose_inv"])
    # 补齐卖出的行为
    c_a_item = {}
    c_a_item['open_date'] = pos['open_date']
    c_a_item['open_qty'] = pre_unclose_qty - qty
    c_a_item['open_amt'] = pre_unclose_amt - mkt_val + float(pos['gain_return'])
    c_a_item['close_inv'] = float(pos['unclose_inv']) * ratio
    c_a_item['close_qty'] = pre_unclose_qty - qty
    c_a_item['close_amt'] = pre_unclose_amt * ratio + mkt_val - (
        1 - ratio) * pre_unclose_amt + float(pos['gain_return'])
    c_a_item['unclose_qty'] = 0
    c_a_item['unclose_amt'] = 0.
    c_a_item['exception_label'] = 1
    c_a_item['return'] = float(c_a_item['close_amt']) - c_a_item['close_inv']
    c_a_item['return_rate'] = c_a_item['return'] / c_a_item['close_inv'] if \
        c_a_item['close_inv'] != 0 else 0.
    c_a_item['close_date'] = busi_date
    c_a_item['close_timestamp'] = int(round(time.time() * 1000))
    c_a_item['exception_label'] = 1
    c_a_item["weighted_term"] = abs(float(pos["weighted_term"]) * ratio)
    c_a_item['close_type'] = 'trade_exception'
    close_detail.append(c_a_item)
    qty = 0
    mkt_val = 0.0
    return qty, mkt_val


def long_compute(data, busi_date, yesterday):
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
    now_trd = data["now_trd"] if data["now_trd"] is not None else []
    now_trd = sorted(now_trd, key=lambda x: (x["timestamp"], -x["trd_qty"]))
    open_ind = 0
    trd_ind = 0
    qty = data["qty"] if data["qty"] is not None else 0
    mkt_val = data["mkt_val"] if data["mkt_val"] is not None else 0.0
    close_detail = []
    while trd_ind < len(now_trd):
        trd = now_trd[trd_ind]
        if trd["amortize_label"] == 1:
            amortize_long_trade(open_detail, open_ind, trd)
            trd_ind += 1
        elif trd["trd_qty"] > 0:
            open_detail.append(trd_init_trans(trd, busi_date))
            trd_ind += 1
        elif trd["trd_qty"] < 0:
            close_item = get_last_close_item(close_detail, trd, busi_date)
            if open_ind < len(open_detail):
                pos = open_detail[open_ind]
                pos, trd, open_ind, trd_ind = compute_long_trade(pos, trd,
                                                                 open_ind,
                                                                 trd_ind,
                                                                 close_item,
                                                                 busi_date)
            else:
                close_item = close_detail[-1]
                close_item['return'] = 0.
                close_item['return_rate'] = 0.
                close_item['exception_label'] = 1
                trd_ind += 1
    # 计算结果和持仓结果进行校验
    for pos in reversed(open_detail[open_ind:]):
        # 这里加1是为了处理进位问题
        if int(pos["unclose_qty"]) <= qty + tol_bias:
            if abs(int(pos["unclose_qty"]) - qty) <= tol_bias:
                current_qty = qty
            else:
                current_qty = int(pos["unclose_qty"])
            pos["unclose_qty"] = str(current_qty)
            if qty != 0:
                pos["unclose_amt"] = str(float(current_qty) / qty * mkt_val)
                mkt_val *= (qty - float(current_qty)) / qty
                qty -= current_qty
            else:
                pos["unclose_amt"] = str(0.0)
                mkt_val = 0.0
                qty = 0
        else:
            qty, mkt_val = _except_long_qty(pos, close_detail, qty, mkt_val,
                                            busi_date)
    # 持仓异常处理
    if qty > tol_bias:
        open_detail.append(
            dict(open_date=busi_date,
                 open_timestamp=str(get_timestamp(busi_date, 15) + 1),
                 open_type="trade_exception",
                 orig_trd_qty=str(qty),
                 orig_trd_amt=str(mkt_val),
                 trd_qty=str(qty),
                 trd_amt=str(mkt_val),
                 close_amt=str(0.0),
                 unclose_inv=str(mkt_val),
                 gain_return=str(0.),
                 unclose_qty=str(qty),
                 unclose_amt=str(mkt_val),
                 weighted_term=str(0.0),
                 exception_label=str(1)))
    update_open_detail_current_day_wt(open_detail)
    a["open_detail"] = open_detail
    a["close_detail"] = map(alltype2str, close_detail)
    return a


class longCloseTradeReturn(computeTemplate):

    def __init__(self, spark):
        computeTemplate.__init__(self, spark)
        self.close_table = self.conf["long_c_close_trade_table"]
        self.unclose_table = self.conf["long_c_unclose_trade_table"]
        self.unclose_cal = self.conf["long_c_unclose_trade_cal"]
        self.asset_table = self.conf["asset_table"]
        self.cash_flow_table = self.conf["cash_flow_table"]

    def init_data(self, busi_date):
        spark = self.spark
        command = "select * from %s.%s where busi_date='%s'" % (
            self.odata, self.asset_table, busi_date)
        data = spark.sql(command).filter("prd_no!='0.0'")
        data = data.rdd.map(lambda x: _init_trans(x.asDict(), "long"))
        self.save_trade_unclose_data(data, spark, busi_date, lambda t: int(t["unclose_qty"]) > 0)

    def daily_compute(self, busi_date):
        """
        """
        spark = self.spark
        yesterday = get_date(self.date_order, self.order_date, busi_date, -1)
        unclose = spark.sql("select * from %s.%s where busi_date='%s'" % (
            self.fdata, self.unclose_cal, yesterday))
        trd = spark.sql("select * from %s.%s where busi_date='%s'" % (
            self.odata, self.cash_flow_table, busi_date))\
            .filter("trd_type='long_related' and prd_no!='0.0'")\
            .filter("trd_qty!=0 or amortize_label!=0")\
            .withColumn("now_trd", func.struct(
                "timestamp", "trd_qty", "trd_cash_flow", "amortize_label"))\
            .groupBy("trade_id", "secu_acc_id", "prd_no")\
            .agg(func.collect_list("now_trd").alias("now_trd"))\
            .persist(StorageLevel.DISK_ONLY)
        asset = spark.sql("select * from %s.%s where busi_date='%s'" % (
            self.odata, self.asset_table, busi_date))\
            .filter("prd_no!='0.0'")\
            .select("trade_id", "secu_acc_id", "prd_no", "qty", "mkt_val")\
            .persist(StorageLevel.DISK_ONLY)
        trd.count()
        asset.count()
        df = full_outer_join(unclose, trd,
                             ["trade_id", "secu_acc_id", "prd_no"])
        df = full_outer_join(df, asset, ["trade_id", "secu_acc_id", "prd_no"])
        df.persist(StorageLevel.DISK_ONLY).count()
        data = df.rdd.map(
            lambda x: _compute(x.asDict(recursive=True), busi_date, yesterday))
        if data.count() > 0:
            data.persist(StorageLevel.DISK_ONLY).count()
            self.save_trade_close_data(data, spark, busi_date)
            self.save_trade_unclose_data(data, spark, busi_date,
                                         lambda t: int(t["unclose_qty"]) > 0)

    @staticmethod
    def _trans_unclose(data):
        def long_trans_unclose(data, item):
            """
            """
            a = flat_data(data, item)
            a["unclose_qty"] = int(item["unclose_qty"])
            a["unclose_amt"] = float(item["unclose_amt"])
            a["close_qty"] = a["trd_qty"] - a["unclose_qty"]
            if a["trd_amt"] == 0:
                a["return"] = 0.0
                a["return_rate"] = 0.0
            else:
                a["return"] = a["close_amt"] + a["unclose_amt"] - a["trd_amt"]
                a["return_rate"] = a["return"] / a["trd_amt"]
            return a

        for item in data["open_detail"]:
            a = long_trans_unclose(data, item)
            yield Row(**a)
