# -*- coding: utf-8 -*-


from leek.common.util import days_between

import calendar
from datetime import datetime, timedelta


def sort_long_close_data(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "open_date",
        "open_timestamp",
        "open_type",
        "close_date",
        "busi_date",
        "orig_trd_qty",
        "orig_trd_amt",
        "trd_qty",
        "trd_amt",
        "close_amt",
        "return",
        "return_rate",
        "weighted_term",
        "exception_label")
    return data


def sort_long_unclose_data(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "open_date",
        "open_timestamp",
        "open_type",
        "busi_date",
        "orig_trd_qty",
        "orig_trd_amt",
        "trd_qty",
        "trd_amt",
        "close_qty",
        "close_amt",
        "unclose_qty",
        "unclose_amt",
        "return",
        "return_rate",
        "weighted_term",
        "exception_label")
    return data


def sort_long_unclose_cal(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "busi_date",
        "open_detail")
    return data


def sort_short_close_data(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "open_date",
        "open_timestamp",
        "open_type",
        "close_date",
        "busi_date",
        "orig_trd_qty",
        "orig_trd_amt",
        "trd_qty",
        "trd_amt",
        "close_amt",
        "return",
        "return_rate",
        "weighted_term",
        "exception_label")
    return data


def sort_short_unclose_data(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "open_date",
        "open_timestamp",
        "open_type",
        "busi_date",
        "orig_trd_qty",
        "orig_trd_amt",
        "trd_qty",
        "trd_amt",
        "close_qty",
        "close_amt",
        "unclose_qty",
        "unclose_amt",
        "return",
        "return_rate",
        "weighted_term",
        "exception_label")
    return data


def sort_short_unclose_cal(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "busi_date",
        "open_detail")
    return data


def filter_data(data, filter_func):
    """
    """
    a = {}
    a["trade_id"] = data["trade_id"]
    a["secu_acc_id"] = data["secu_acc_id"]
    a["prd_no"] = data["prd_no"]
    a["busi_date"] = data["busi_date"]
    a["open_detail"] = filter(filter_func, data["open_detail"])
    return a


def get_timestamp(busi_date, hour):
    """
    """
    d = datetime.strptime(busi_date, "%Y-%m-%d") + timedelta(hours=hour)
    # 时区为GMT +8
    return (calendar.timegm(d.timetuple()) - 8 * 60 * 60) * 1000


def trd_init_trans(trd, busi_date):
    """
    """
    a = {"open_date": busi_date,
         "open_timestamp": str(trd["timestamp"]),
         "open_type": "trade",
         "orig_trd_qty": str(trd["trd_qty"]),
         "orig_trd_amt": str(trd["trd_cash_flow"]),
         "trd_qty": str(trd["trd_qty"]),
         "trd_amt": str(trd["trd_cash_flow"]),
         "close_amt": str(0.0),
         "unclose_qty": str(trd["trd_qty"]),
         "unclose_amt": str(trd["trd_cash_flow"]),
         "weighted_term": str(0.0),
         "exception_label": str(0)}
    return a


def flat_data(data, item):
    """
    """
    a = {}
    a["trade_id"] = data["trade_id"]
    a["secu_acc_id"] = data["secu_acc_id"]
    a["prd_no"] = data["prd_no"]
    a["open_date"] = item["open_date"]
    a["open_timestamp"] = int(item["open_timestamp"])
    a["open_type"] = item["open_type"]
    a["busi_date"] = data["busi_date"]
    a["orig_trd_qty"] = int(item["orig_trd_qty"])
    a["orig_trd_amt"] = float(item["orig_trd_amt"])
    a["trd_qty"] = int(item["trd_qty"])
    a["trd_amt"] = float(item["trd_amt"])
    a["close_amt"] = float(item["close_amt"])
    a["weighted_term"] = float(item["weighted_term"])
    a["exception_label"] = int(item["exception_label"])
    return a


def long_trans_close(data, item):
    """
    """
    a = flat_data(data, item)
    a["close_date"] = data["busi_date"]
    if a["trd_amt"] != 0:
        a["return"] = a["close_amt"] - a["trd_amt"]
        a["return_rate"] = a["return"] / a["trd_amt"]
    else:
        a["return"] = 0.0
        a["return_rate"] = 0.
    return a


def long_trans_unclose(data, item):
    """
    """
    a = flat_data(data, item)
    a["unclose_qty"] = int(item["unclose_qty"])
    a["unclose_amt"] = float(item["unclose_amt"])
    a["close_qty"] = a["trd_qty"] - a["unclose_qty"]
    if a["trd_amt"] != 0:
        a["return"] = a["close_amt"] + a["unclose_amt"] - a["trd_amt"]
        a["return_rate"] = a["return"] / a["trd_amt"]
    else:
        a["return"] = 0.0
        a["return_rate"] = 0.0
    return a


def short_trans_close(data, item):
    """
    """
    a = flat_data(data, item)
    a["close_date"] = data["busi_date"]
    if a["close_amt"] != 0 :
        a["return"] = -a["trd_amt"] - a["close_amt"]
        a["return_rate"] = a["return"] / a["close_amt"]
    else:
        a["return"] = 0.0
        a["return_rate"] =0.0
    return a


def short_trans_unclose(data, item):
    """
    """
    a = flat_data(data, item)
    a["unclose_qty"] = int(item["unclose_qty"])
    a["unclose_amt"] = float(item["unclose_amt"])
    a["close_qty"] = a["trd_qty"] - a["unclose_qty"]
    if a["close_amt"] - a["unclose_amt"] != 0:
        a["return"] = -a["trd_amt"] - (a["close_amt"] - a["unclose_amt"])
        a["return_rate"] = a["return"] / (a["close_amt"] - a["unclose_amt"])
    else:
        a["return"] = 0.0
        a["return_rate"] =0.0
    return a


def amortize_long_trade(open_detail, open_ind, trd):
    """
    """
    _qty_sum = sum([int(i["unclose_qty"]) for i in open_detail[open_ind:]])
    for item in open_detail[open_ind:]:
        ratio = float(item["unclose_qty"]) / _qty_sum
        item["trd_qty"] = str(
            int(item["trd_qty"]) + int(round(ratio * trd["trd_qty"])))
        item["unclose_qty"] = str(
            int(item["unclose_qty"]) + int(round(ratio * trd["trd_qty"])))
        if trd["trd_cash_flow"] > 0:
            item["trd_amt"] = str(
                float(item["trd_amt"]) + ratio * trd["trd_cash_flow"])
        else:
            item["close_amt"] = str(
                float(item["close_amt"]) - ratio * trd["trd_cash_flow"])
    return open_detail


def compute_long_trade(pos, trd, open_ind, trd_ind):
    """
    """
    # 注意：trd["trd_qty"]是一个负数
    if int(pos["unclose_qty"]) + trd["trd_qty"] >= 0:
        _qty = int(pos["unclose_qty"])
        pos["unclose_qty"] = str(_qty + trd["trd_qty"])
        pos["close_amt"] = str(
            float(pos["close_amt"]) - trd["trd_cash_flow"])
        pos["unclose_amt"] = str(
            float(_qty + trd["trd_qty"]) / _qty * float(pos["unclose_amt"]))
        trd_ind += 1
        if int(pos["unclose_qty"]) == 0:
            open_ind += 1
    else:
        _qty = int(pos["unclose_qty"])
        pos["close_amt"] = str(
            float(pos["close_amt"]) + trd["trd_cash_flow"] * _qty / trd["trd_qty"])
        trd["trd_cash_flow"] *= 1 + float(_qty) / trd["trd_qty"]
        trd["trd_qty"] += _qty
        pos["unclose_qty"] = str(0)
        pos["unclose_amt"] = str(0.0)
        open_ind += 1
    return pos, trd, open_ind, trd_ind


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
    for pos in open_detail:
        # 这里先计算是因为后面的unclose_qty会变
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = (days_between(busi_date, yesterday) - 1) * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)
    now_trd = data["now_trd"] if data["now_trd"] is not None else []
    now_trd = sorted(now_trd, key=lambda x: (x["timestamp"], -x["trd_qty"]))
    open_ind = 0
    trd_ind = 0
    qty = data["qty"] if data["qty"] is not None else 0
    mkt_val = data["mkt_val"] if data["mkt_val"] is not None else 0.0
    while trd_ind < len(now_trd):
        trd = now_trd[trd_ind]
        if trd["amortize_label"] == 1:
            open_detail = amortize_long_trade(open_detail, open_ind, trd)
            trd_ind += 1
        elif trd["trd_qty"] > 0:
            open_detail.append(trd_init_trans(trd, busi_date))
            trd_ind += 1
        elif trd["trd_qty"] < 0:
            if open_ind < len(open_detail):
                pos = open_detail[open_ind]
                pos, trd, open_ind, trd_ind = \
                    compute_long_trade(pos, trd, open_ind, trd_ind)
            else:
                # 数据出错了，忽略卖出行为
                trd_ind += 1
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
            pos["exception_label"] = str(1)
            # 异常处理一下
            ratio = (int(pos["unclose_qty"]) - qty) / float(pos["unclose_qty"])
            pos["close_amt"] = str(
                float(pos["close_amt"]) + float(pos["unclose_amt"]) * ratio)
            pos["unclose_qty"] = str(qty)
            pos["unclose_amt"] = str(mkt_val)
            qty = 0
            mkt_val = 0.0
    # 持仓异常处理
    if qty > tol_bias:
        open_detail.append({
            "open_date": busi_date,
            "open_timestamp": str(get_timestamp(busi_date, 15) + 1),
            "open_type": "trade_exception",
            "orig_trd_qty": str(qty),
            "orig_trd_amt": str(mkt_val),
            "trd_qty": str(qty),
            "trd_amt": str(mkt_val),
            "close_amt": str(0.0),
            "unclose_qty": str(qty),
            "unclose_amt": str(mkt_val),
            "weighted_term": str(0.0),
            "exception_label": str(1)})
    a["open_detail"] = open_detail
    # 更新持仓周期
    for pos in open_detail:
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = 1 * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)
    return a


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
        if trd["trd_cash_flow"] > 0:
            item["close_amt"] = str(
                float(item["close_amt"]) + ratio * trd["trd_cash_flow"])
        else:
            item["trd_amt"] = str(
                float(item["trd_amt"]) + ratio * trd["trd_cash_flow"])
    return open_detail


def compute_short_trade(pos, trd, open_ind, trd_ind):
    """
    """
    # 注意：trd["trd_qty"]是一个正数
    if int(pos["unclose_qty"]) + trd["trd_qty"] <= 0:
        _qty = int(pos["unclose_qty"])
        pos["unclose_qty"] = str(_qty + trd["trd_qty"])
        pos["close_amt"] = str(
            float(pos["close_amt"]) + trd["trd_cash_flow"])
        pos["unclose_amt"] = str(
            float(_qty + trd["trd_qty"]) / _qty * float(pos["unclose_amt"]))
        trd_ind += 1
        if int(pos["unclose_qty"]) == 0:
            open_ind += 1
    else:
        _qty = int(pos["unclose_qty"])
        pos["close_amt"] = str(
            float(pos["close_amt"])
            + trd["trd_cash_flow"] * (-1 * _qty) / trd["trd_qty"])
        trd["trd_cash_flow"] *= 1 + float(_qty) / trd["trd_qty"]
        trd["trd_qty"] += _qty
        pos["unclose_qty"] = str(0)
        pos["unclose_amt"] = str(0.0)
        open_ind += 1
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
    for pos in open_detail:
        # 这里先计算是因为后面的unclose_qty会变
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = (days_between(busi_date, yesterday) - 1) * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)
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
            if open_ind < len(open_detail):
                pos = open_detail[open_ind]
                pos, trd, open_ind, trd_ind = \
                    compute_short_trade(pos, trd, open_ind, trd_ind)
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
                pos["unclose_amt"] = str(float(current_qty) / liab_qty * mkt_val)
                mkt_val *= (liab_qty - float(current_qty)) / liab_qty
                liab_qty -= current_qty
            else:
                pos["unclose_amt"] = str(0.0)
                mkt_val = 0.0
                liab_qty = 0
        else:
            pos["exception_label"] = str(1)
            # 异常处理一下
            ratio = (int(pos["unclose_qty"]) - liab_qty) / float(pos["unclose_qty"])
            pos["close_amt"] = str(
                float(pos["close_amt"]) - float(pos["unclose_amt"]) * ratio)
            pos["unclose_qty"] = str(liab_qty)
            pos["unclose_amt"] = str(mkt_val)
            liab_qty = 0
            mkt_val = 0.0
    # 持仓异常处理
    if liab_qty < -1 * tol_bias:
        open_detail.append({
            "open_date": busi_date,
            "open_timestamp": str(get_timestamp(busi_date, 15) + 1),
            "open_type": "trade_exception",
            "orig_trd_qty": str(liab_qty),
            "orig_trd_amt": str(mkt_val),
            "trd_qty": str(liab_qty),
            "trd_amt": str(mkt_val),
            "close_amt": str(0.0),
            "unclose_qty": str(liab_qty),
            "unclose_amt": str(mkt_val),
            "weighted_term": str(0.0),
            "exception_label": str(1)})
    a["open_detail"] = open_detail
    # 更新持仓周期
    for pos in open_detail:
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = 1 * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)
    return a

