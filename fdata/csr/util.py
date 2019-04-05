# -*- coding: utf-8 -*-


from leek.common.util import days_between


def sort_short_unclose_data(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "busi_date",
        "open_date",
        "holding_term",
        "return",
        "return_rate",
        "total_in",
        "total_out",
        "remain_liab_qty",
        "remain_val",
        "trd_detail",
        "exception_label")
    return data


def sort_short_close_data(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "close_date",
        "busi_date",
        "open_date",
        "holding_term",
        "return",
        "return_rate",
        "total_in",
        "total_out",
        "trd_detail",
        "exception_label")
    return data


def sort_long_unclose_data(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "busi_date",
        "open_date",
        "holding_term",
        "return",
        "return_rate",
        "total_in",
        "total_out",
        "remain_qty",
        "remain_val",
        "trd_detail",
        "exception_label")
    return data


def sort_long_close_data(data):
    """
    """
    data = data.select(
        "trade_id",
        "secu_acc_id",
        "prd_no",
        "close_date",
        "busi_date",
        "open_date",
        "holding_term",
        "return",
        "return_rate",
        "total_in",
        "total_out",
        "trd_detail",
        "exception_label")
    return data


def long_compute(data, busi_date):
    """
    """
    a = {}
    trd_detail = data["trd_detail"] if data["trd_detail"] is not None else []
    a["trade_id"] = data["trade_id"]
    a["secu_acc_id"] = data["secu_acc_id"]
    a["prd_no"] = data["prd_no"]
    a["busi_date"] = busi_date
    if data["open_date"] is None:
        a["open_date"] = busi_date
        a["holding_term"] = 1L
    else:
        a["open_date"] = data["open_date"]
        a["holding_term"] = days_between(busi_date, data["open_date"]) + 1L
    x = 0
    y = 0.0
    total_in = 0.0
    total_out = 0.0
    _qty = 0
    for i in trd_detail:
        _qty += int(i["trd_qty"])
        if float(i["trd_amt"]) >= 0:
            total_in += float(i["trd_amt"])
        else:
            total_out += -float(i["trd_amt"])
        if int(i["trd_qty"]) >= 0:
            x += int(i["trd_qty"])
            y += float(i["trd_amt"])
        else:
            # 出现异常的时候，处理一下
            y *= float(max(x + int(i["trd_qty"]), 0)) / x if x != 0 else 0.0
            x = max(x + int(i["trd_qty"]), 0)
    now_trd = data["now_trd"] if data["now_trd"] is not None else []
    now_trd = sorted(now_trd, key=lambda x: (x["timestamp"], -x["trd_qty"]))
    for i in now_trd:
        _qty += i["trd_qty"]
        if i["trd_cash_flow"] >= 0:
            total_in += i["trd_cash_flow"]
        else:
            total_out += -i["trd_cash_flow"]
        item = {}
        item["trd_date"] = busi_date
        item["trd_qty"] = str(i["trd_qty"])
        item["trd_amt"] = str(i["trd_cash_flow"])
        if i["trd_qty"] >= 0:
            x += i["trd_qty"]
            y += i["trd_cash_flow"]
            item["return"] = str(0.0)
        else:
            # 出现异常的时候，处理一下
            cost = y / x if x != 0 else abs(float(i["trd_cash_flow"]) / i["trd_qty"])
            item["return"] = str(abs(i["trd_cash_flow"]) - abs(cost * i["trd_qty"]))
            # 出现异常的时候，处理一下
            y *= float(max(x + i["trd_qty"], 0)) / x if x != 0 else 0.0
            x = max(x + i["trd_qty"], 0)
        trd_detail.append(item)
    a["trd_detail"] = trd_detail
    a["total_in"] = total_in
    a["total_out"] = total_out
    a["remain_qty"] = data["qty"] if data["qty"] is not None else 0
    a["remain_val"] = data["mkt_val"] if data["mkt_val"] is not None else 0.0
    a["exception_label"] = int(_qty != a["remain_qty"])
    a["return"] = total_out + a["remain_val"] - total_in
    a["return_rate"] = a["return"] / total_in if total_in != 0 else 0.0
    return a


def short_compute(data, busi_date):
    """
    """
    a = {}
    trd_detail = data["trd_detail"] if data["trd_detail"] is not None else []
    a["trade_id"] = data["trade_id"]
    a["secu_acc_id"] = data["secu_acc_id"]
    a["prd_no"] = data["prd_no"]
    a["busi_date"] = busi_date
    if data["open_date"] is None:
        a["open_date"] = busi_date
        a["holding_term"] = 1L
    else:
        a["open_date"] = data["open_date"]
        a["holding_term"] = days_between(busi_date, data["open_date"]) + 1L
    x = 0
    y = 0.0
    total_in = 0.0
    total_out = 0.0
    _qty = 0
    for i in trd_detail:
        _qty += int(i["trd_qty"])
        if float(i["trd_amt"]) >= 0:
            total_in += float(i["trd_amt"])
        else:
            total_out += -float(i["trd_amt"])
        if int(i["trd_qty"]) <= 0:
            x += int(i["trd_qty"])
            y += float(i["trd_amt"])
        else:
            # 出现异常的时候，处理一下
            y *= float(min(x + int(i["trd_qty"]), 0)) / x if x != 0 else 0.0
            x = min(x + int(i["trd_qty"]), 0)
    now_trd = data["now_trd"] if data["now_trd"] is not None else []
    now_trd = sorted(now_trd, key=lambda x: (x["timestamp"], x["trd_qty"]))
    for i in now_trd:
        _qty += i["trd_qty"]
        if i["trd_cash_flow"] >= 0:
            total_in += i["trd_cash_flow"]
        else:
            total_out += -i["trd_cash_flow"]
        item = {}
        item["trd_date"] = busi_date
        item["trd_qty"] = str(i["trd_qty"])
        item["trd_amt"] = str(i["trd_cash_flow"])
        if i["trd_qty"] <= 0:
            x += i["trd_qty"]
            y += i["trd_cash_flow"]
            item["return"] = str(0.0)
        else:
            # 出现异常的时候，处理一下
            revenue = y / x if x != 0 else abs(float(i["trd_cash_flow"]) / i["trd_qty"])
            item["return"] = str(revenue * i["trd_qty"] - i["trd_cash_flow"])
            # 出现异常的时候，处理一下
            y *= float(min(x + int(i["trd_qty"]), 0)) / x if x != 0 else 0.0
            x = min(x + int(i["trd_qty"]), 0)
        trd_detail.append(item)
    a["trd_detail"] = trd_detail
    a["total_in"] = total_in
    a["total_out"] = total_out
    a["remain_liab_qty"] = data["liab_qty"] if data["liab_qty"] is not None else 0
    a["remain_val"] = data["mkt_val"] if data["mkt_val"] is not None else 0.0
    a["exception_label"] = int(_qty != a["remain_liab_qty"])
    a["return"] = total_out + a["remain_val"] - total_in
    if total_in + abs(a["remain_val"]) != 0:
        a["return_rate"] = a["return"] / (total_in + abs(a["remain_val"]))
    else:
        a["return_rate"] = 0.0
    return a
