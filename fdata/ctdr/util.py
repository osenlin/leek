# -*- coding: utf-8 -*-


from leek.common.util import days_between
import calendar
from datetime import datetime, timedelta


def sort_close_data(data):
    """
    """
    data = data.select("trade_id", "secu_acc_id", "prd_no", "open_date",
                       "close_date", "busi_date", "orig_trd_qty",
                       "orig_trd_amt", "trd_qty", "trd_amt", "close_amt",
                       "return", "return_rate", "weighted_term",
                       "exception_label")
    return data


def sort_unclose_data(data):
    """
    """
    data = data.select("trade_id", "secu_acc_id", "prd_no", "open_date",
                       "busi_date", "orig_trd_qty", "orig_trd_amt", "trd_qty",
                       "trd_amt", "close_qty", "close_amt", "unclose_qty",
                       "unclose_amt", "return", "return_rate", "weighted_term",
                       "exception_label")
    return data


def sort_unclose_cal(data):
    """
    """
    data = data.select("trade_id", "secu_acc_id", "prd_no", "busi_date",
                       "open_detail")
    return data


def flat_data(data, item):
    """
    """
    a = dict(trade_id=data["trade_id"], secu_acc_id=data["secu_acc_id"],
             prd_no=data["prd_no"], open_date=item["open_date"],
             open_type=item["open_type"], busi_date=data["busi_date"],
             orig_trd_qty=int(item["orig_trd_qty"]),
             orig_trd_amt=float(item["orig_trd_amt"]),
             trd_qty=int(item["trd_qty"]), trd_amt=float(item["trd_amt"]),
             close_amt=float(item["close_amt"]),
             weighted_term=float(item["weighted_term"]),
             exception_label=int(item["exception_label"]))
    return a


def get_timestamp(busi_date, hour):
    """
    """
    d = datetime.strptime(busi_date, "%Y-%m-%d") + timedelta(hours=hour)
    # 时区为GMT +8
    return (calendar.timegm(d.timetuple()) - 8 * 60 * 60) * 1000


def trd_init_trans(trd, busi_date, exception_label=0):
    """
    """
    a = {"open_date": busi_date, "open_type": "trade",
         "orig_trd_qty": str(trd["trd_qty"]),
         "orig_trd_amt": str(trd["trd_cash_flow"]),
         "trd_qty": str(trd["trd_qty"]), "trd_amt": str(trd["trd_cash_flow"]),
         "close_amt": str(0.0), "unclose_qty": str(trd["trd_qty"]),
         "unclose_amt": str(trd["trd_cash_flow"]), "weighted_term": str(0.0),
         "exception_label": str(exception_label)}
    return a


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


def trd_add_trans(open_detail, trd, busi_date, qty, open_ind):
    len_open_detail = len(open_detail)
    if len_open_detail == 0:
        open_detail.append(trd_init_trans(trd, busi_date))
    elif len(open_detail) > 0 and open_detail[-1]["open_date"] != busi_date:
        open_detail.append(trd_init_trans(trd, busi_date))
    elif len(open_detail) > 0 and open_detail[-1]["open_date"] == busi_date:
        before = open_detail[-1]
        #针对T+0的情况，需要打开已经完结的交易，继续处理
        if int(before["unclose_qty"]) == 0:
            open_ind -= 1
        before['trd_qty'] = str(long(before["trd_qty"]) + long(trd["trd_qty"]))
        before['trd_amt'] = str(
            float(before["trd_amt"]) + float(trd["trd_cash_flow"]))
        before['unclose_qty'] = str(
            long(before["unclose_qty"]) + long(trd["trd_qty"]))
        before['unclose_amt'] = str(
            float(before["unclose_amt"]) + float(trd["trd_cash_flow"]))
        before['orig_trd_qty'] = str(
            long(before["orig_trd_qty"]) + long(trd["trd_qty"]))
        before['orig_trd_amt'] = str(
            float(before["orig_trd_amt"]) + float(trd["trd_cash_flow"]))
    return open_ind


def amortize_long_trade(open_detail, open_ind, trd):
    """
    存在分股分红的情况下，向前平摊
    """
    _qty_sum = sum([int(i["trd_qty"]) for i in open_detail[open_ind:]])
    for item in open_detail[open_ind:]:
        ratio = float(item["trd_qty"]) / _qty_sum
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


def amortize_short_trade(open_detail, open_ind, trd):
    """
    做空的平摊规则？
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


def compute_long_trade(pos, trd, open_ind, trd_ind):
    """
    """
    # 注意：trd["trd_qty"]是一个负数
    if int(pos["unclose_qty"]) + trd["trd_qty"] >= 0:
        _qty = int(pos["unclose_qty"])
        pos["unclose_qty"] = str(_qty + trd["trd_qty"])
        pos["close_amt"] = str(float(pos["close_amt"]) - trd["trd_cash_flow"])
        pos["unclose_amt"] = str(
            float(_qty + trd["trd_qty"]) / _qty * float(pos["unclose_amt"]))
        trd_ind += 1
        if int(pos["unclose_qty"]) == 0:
            open_ind += 1
    else:
        _qty = int(pos["unclose_qty"])
        pos["close_amt"] = str(
            float(pos["close_amt"]) + trd["trd_cash_flow"] * _qty / trd[
                "trd_qty"])
        trd["trd_cash_flow"] = float(trd["trd_cash_flow"]) * (
            1 + float(_qty) / trd["trd_qty"])
        trd["trd_qty"] = int(trd["trd_qty"]) + _qty
        pos["unclose_qty"] = str(0)
        pos["unclose_amt"] = str(0.0)
        open_ind += 1
    return pos, trd, open_ind, trd_ind


def compute_short_trade(pos, trd, open_ind, trd_ind):
    """
    """
    # 注意：trd["trd_qty"]是一个正数
    if int(pos["unclose_qty"]) + trd["trd_qty"] <= 0:
        _qty = int(pos["unclose_qty"])
        pos["unclose_qty"] = str(_qty + trd["trd_qty"])
        pos["close_amt"] = str(float(pos["close_amt"]) + trd["trd_cash_flow"])
        pos["unclose_amt"] = str(
            float(_qty + trd["trd_qty"]) / _qty * float(pos["unclose_amt"]))
        trd_ind += 1
        if int(pos["unclose_qty"]) == 0:
            open_ind += 1
    else:
        _qty = int(pos["unclose_qty"])
        pos["close_amt"] = str(
            float(pos["close_amt"]) + trd["trd_cash_flow"] * (-1 * _qty) / trd[
                "trd_qty"])
        trd["trd_cash_flow"] *= 1 + float(_qty) / trd["trd_qty"]
        trd["trd_qty"] += _qty
        pos["unclose_qty"] = str(0)
        pos["unclose_amt"] = str(0.0)
        open_ind += 1
    return pos, trd, open_ind, trd_ind
