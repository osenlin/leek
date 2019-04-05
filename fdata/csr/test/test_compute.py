# -*- coding: utf-8 -*-


from leek.fdata.csr.test.testing import assert_less, assert_array_almost_equal, assert_dict_equal,assert_equal
from leek.fdata.csr.util import long_compute
from leek.fdata.csr.util import short_compute


#需要走的逻辑，trd_detail 顺序有一定问题的情况下
def test_long_compute():
    """
    做多场景1，考虑正常情况下，在计算日无交易
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data['qty']=950
    data['mkt_val']=10000.
    data["trd_detail"] = [
        {
            "trd_amt": "8000",
            "trd_qty": "800",
            "trd_date": "2017-03-10",
            "return": "0.0"},
        {
            "trd_amt": "2000",
            "trd_qty": "150",
            "trd_date": "2017-03-12",
            "return": "0.0"}
    ]
    data['now_trd']=None
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = long_compute(data, busi_date)
    assert_equal(re['total_in'],10000.)
    assert_equal(re['total_out'], 0.)
    assert_equal(re['remain_val'], 10000.)
    assert_equal(re['remain_qty'], 950)


def test_long_compute2():
    """
    做多场3，考虑金额异常卖出
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data['qty']=950
    data['mkt_val']=20000.
    data["trd_detail"] = [
        {
            "trd_amt": "8000",
            "trd_qty": "800",
            "trd_date": "2017-03-10",
            "return": "0.0"},
        {
            "trd_amt": "2000",
            "trd_qty": "150",
            "trd_date": "2017-03-12",
            "return": "0.0"}
    ]
    data['now_trd']=[
        {
            "trd_qty":-800,
            "trd_cash_flow":-10000,
            "timestamp":1489626002
        },
        {
            "trd_qty": -150,
            "trd_cash_flow": -20000,
            "timestamp": 1489626001
        }
    ]
    data['qty']=0
    data['mkt_val']=0.
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = long_compute(data, busi_date)
    assert_equal(re['return_rate'],2.)
    assert_equal(re['total_in'], 10000.)
    assert_equal(re['total_out'], 30000.)
    assert_equal(re['exception_label'], 0)


def test_long_compute3():
    """
    做多场4，考虑数量异常卖出
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data["trd_detail"] = [
        {
            "trd_amt": "8000",
            "trd_qty": "800",
            "trd_date": "2017-03-10",
            "return": "0.0"},
        {
            "trd_amt": "2000",
            "trd_qty": "150",
            "trd_date": "2017-03-12",
            "return": "0.0"}
    ]
    data['now_trd']=[
        {
            "trd_qty":-1000,
            "trd_cash_flow":-5000,
            "timestamp":1489626001
        },
        {
            "trd_qty": -150,
            "trd_cash_flow": -5000,
            "timestamp": 1489626002
        }
    ]
    data['qty']=0
    data['mkt_val']=0.
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = long_compute(data, busi_date)
    assert_equal(re['return_rate'],0.)
    assert_equal(re['total_in'], 10000.)
    assert_equal(re['total_out'], 10000.)
    assert_equal(re['remain_qty'], 0)
    assert_equal(re['remain_val'], 0.)
    assert_equal(re['exception_label'], 1)
    assert_less(-5526.31578947 - float(re["trd_detail"][2]["return"]), 1e-4)
    assert_less(0.0 - float(re["trd_detail"][3]["return"]), 1e-4)


def test_long_compute4():
    """
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data["trd_detail"] = []
    data['now_trd']=[
        {
            "trd_qty":-1000,
            "trd_cash_flow":-5000,
            "timestamp":1489626000
        },
        {
            "trd_qty": 150,
            "trd_cash_flow": 4000,
            "timestamp": 1489626001
        },
        {
            "trd_qty": -150,
            "trd_cash_flow": -5000,
            "timestamp": 1489626002
        }
    ]
    data['qty']=0
    data['mkt_val']=0.
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = long_compute(data, busi_date)
    assert_less(0.0 - float(re["trd_detail"][0]["return"]), 1e-4)
    assert_less(1000.0 - float(re["trd_detail"][2]["return"]), 1e-4)


#需要走的逻辑，trd_detail 顺序有一定问题的情况下
def test_short_compute():
    """
    做空场景1，考虑正常情况下，在计算日无交易
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data['liab_qty']=-950
    data['mkt_val']=-10000.
    data["trd_detail"] = [
        {
            "trd_amt": "-8000",
            "trd_qty": "-800",
            "trd_date": "2017-03-10",
            "return": "0.0"},
        {
            "trd_amt": "-2000",
            "trd_qty": "-150",
            "trd_date": "2017-03-12",
            "return": "0.0"}
    ]
    data['now_trd']=None
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = short_compute(data, busi_date)
    assert_equal(re['total_in'],0.)
    assert_equal(re['total_out'], 10000.)
    assert_equal(re['remain_val'], -10000.)
    assert_equal(re['remain_liab_qty'],-950)


def test_short_compute2():
    """
    做多场2，考虑金额正常卖出
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data['liab_qty']=0
    data['mkt_val']=0.
    data["trd_detail"] = [
        {
            "trd_amt": "-8000",
            "trd_qty": "-800",
            "trd_date": "2017-03-10",
            "return": "0.0"},
        {
            "trd_amt": "-2000",
            "trd_qty": "-150",
            "trd_date": "2017-03-12",
            "return": "0.0"}
    ]
    data['now_trd']=[
        {
            "trd_qty":800,
            "trd_cash_flow":5000,
            "timestamp":1489626002
        },
        {
            "trd_qty": 150,
            "trd_cash_flow": 5000,
            "timestamp": 1489626001
        }
    ]
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = short_compute(data, busi_date)
    assert_equal(re['return_rate'],0.)
    assert_equal(re['total_in'], 10000.)
    assert_equal(re['total_out'], 10000.)
    assert_equal(re['exception_label'],0)


def test_short_compute3():
    """
    做多场2，考虑金额正常卖出
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data['liab_qty']=0
    data['mkt_val']=0.
    data["trd_detail"] = [
        {
            "trd_amt": "-8000",
            "trd_qty": "-800",
            "trd_date": "2017-03-10",
            "return": "0.0"},
        {
            "trd_amt": "-2000",
            "trd_qty": "-150",
            "trd_date": "2017-03-12",
            "return": "0.0"}
    ]
    data['now_trd']=[
        {
            "trd_qty":800,
            "trd_cash_flow":5000,
            "timestamp":1489626002
        },
        {
            "trd_qty": 100,
            "trd_cash_flow": 5000,
            "timestamp": 1489626001
        }
    ]
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = short_compute(data, busi_date)
    assert_equal(re['return_rate'],0.)
    assert_equal(re['total_in'], 10000.)
    assert_equal(re['total_out'], 10000.)
    assert_equal(re['exception_label'],1)


def test_short_compute4():
    """
    做多场2，考虑金额正常卖出
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data['liab_qty']=0
    data['mkt_val']=0.
    data["trd_detail"] = [
        {
            "trd_amt": "-8000",
            "trd_qty": "-800",
            "trd_date": "2017-03-10",
            "return": "0.0"},
        {
            "trd_amt": "-2000",
            "trd_qty": "-150",
            "trd_date": "2017-03-12",
            "return": "0.0"}
    ]
    data['now_trd']=[
        {
            "trd_qty":800,
            "trd_cash_flow":4000,
            "timestamp":1489626002
        },
        {
            "trd_qty": 150,
            "trd_cash_flow": 1000,
            "timestamp": 1489626001
        }
    ]
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = short_compute(data, busi_date)
    assert_equal(re['return_rate'],1.)
    assert_equal(re['total_in'], 5000.)
    assert_equal(re['total_out'], 10000.)
    assert_equal(re['exception_label'],0)


def test_short_compute5():
    """
    做多场2，考虑异常情况下的卖出
    """
    busi_date = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data['open_date']="2017-03-10"
    data["busi_date"] = busi_date
    data['liab_qty']=0
    data['mkt_val']=0.
    data["trd_detail"] = []
    data['now_trd']=[
        {
            "trd_qty":800,
            "trd_cash_flow":4000,
            "timestamp":1489626000
        },
        {
            "trd_qty": -150,
            "trd_cash_flow": -1200,
            "timestamp": 1489626001
        },
        {
            "trd_qty": 150,
            "trd_cash_flow": 1000,
            "timestamp": 1489626002
        }
    ]
    data["total_in"] = 0.
    data["total_out"] = 0.
    re = short_compute(data, busi_date)
    assert_equal(re['exception_label'], 1)
    assert_less(0.0 - float(re["trd_detail"][0]["return"]), 1e-4)
    assert_less(0.0 - float(re["trd_detail"][1]["return"]), 1e-4)
    assert_less(200.0 - float(re["trd_detail"][2]["return"]), 1e-4)
