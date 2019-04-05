# -*- coding: utf-8 -*-
from leek.common.util import days_between
import calendar
from datetime import datetime, timedelta


def alltype2str(item):
    a = dict()
    for k, v in item.iteritems():
        a[k] = str(v)
    return a


def flat_data(data, item):
    """
    """
    a = dict()
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


def _init_trans(data, type):
    a = dict(trade_id=data["trade_id"], secu_acc_id=data["secu_acc_id"],
             prd_no=data["prd_no"], busi_date=data["busi_date"])
    qtyName = "qty" if type == 'long' else "liab_qty"
    a["open_detail"] = [
        dict(open_date=data["busi_date"],
             open_timestamp=str(get_timestamp(data["busi_date"], 9)),
             open_type="init",
             orig_trd_qty=str(data[qtyName]),
             orig_trd_amt=str(data["mkt_val"]),
             trd_qty=str(data[qtyName]),
             trd_amt=str(data["mkt_val"]),
             close_amt=str(0.0),
             unclose_inv=str(data["mkt_val"]),
             gain_return=str(0.),
             unclose_qty=str(data[qtyName]),
             unclose_amt=str(data["mkt_val"]),
             weighted_term=str(1.0),
             exception_label=str(0))]
    return a


def update_open_detail_yesterday_wt(open_detail, busi_date, yesterday):
    '''
    计算截止到昨天的加权持仓
    :param open_detail:
    :param busi_date:
    :param yesterday:
    :return:
    '''
    for pos in open_detail:
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = (days_between(busi_date, yesterday) - 1) * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)


def update_open_detail_current_day_wt(open_detail):
    '''
    计算今天的加权持仓
    :param open_detail:
    :return:
    '''
    for pos in open_detail:
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = 1 * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)


def get_timestamp(busi_date, hour):
    d = datetime.strptime(busi_date, "%Y-%m-%d") + timedelta(hours=hour)
    return (calendar.timegm(d.timetuple()) - 8 * 60 * 60) * 1000


def trd_init_trans(trd, busi_date):
    """
    """
    a = {"open_date": busi_date, "open_timestamp": str(trd["timestamp"]),
         "open_type": "trade", "orig_trd_qty": str(trd["trd_qty"]),
         "orig_trd_amt": str(trd["trd_cash_flow"]),
         "trd_qty": str(trd["trd_qty"]), "trd_amt": str(trd["trd_cash_flow"]),
         "close_amt": str(0.0), "gain_return": str(0.0),
         "unclose_qty": str(trd["trd_qty"]),
         "unclose_amt": str(trd["trd_cash_flow"]),
         "unclose_inv": str(trd["trd_cash_flow"]), "weighted_term": str(0.0),
         "exception_label": str(0)}
    return a


def get_last_close_item(close_detail, trd, busi_date):
    if len(close_detail) == 0 or close_detail[-1]['unclose_qty'] == 0:
        close_detail.append({'close_qty': 0, 'close_amt': 0., 'unclose_qty': 0,
                             'unclose_amt': 0., 'close_inv': 0., 'return': 0.,
                             'open_date': "-1",
                             'open_qty': long(trd['trd_qty']),
                             'open_amt': trd['trd_cash_flow'],
                             'return_rate': 0., 'exception_label': long(0),
                             'weighted_term': 0., 'close_date': str(busi_date),
                             'close_timestamp': long(0),
                             'close_type': str('init')})
    close_item = close_detail[-1]
    return close_item
