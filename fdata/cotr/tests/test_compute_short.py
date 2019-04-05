# -*- coding: utf-8 -*-


from leek.fdata.cotr.tests.testing import assert_less, assert_array_almost_equal, assert_dict_equal
from leek.fdata.cotr.util import short_compute, short_trans_close, short_trans_unclose


def test_short_compute():
    """
    分红分股情况下，正常做空
    """
    busi_date = "2017-03-21"
    yesterday = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [
        {"orig_trd_amt": "-1439280.0",
         "orig_trd_qty": "-14400",
         "trd_amt": "-1439280.0",
         "weighted_term": "1.0",
         "trd_qty": "-14400",
         "open_type": "trade_exception",
         "close_amt": "0.0",
         "unclose_qty": "-14400",
         "exception_label": "0",
         "open_date": "2017-03-20",
         "open_timestamp": "1489993201",
         "unclose_amt": "-1439280.0"}]
    data["now_trd"] = [
        {"timestamp": 1490108421,
         "trd_qty": -36000,
         "trd_cash_flow": 0.0,
         "amortize_label": 1},
        {"timestamp": 1490108420,
         "trd_qty": 0,
         "trd_cash_flow": 1440.0,
         "amortize_label": 1}]
    data["liab_qty"] = -50400
    data["mkt_val"] = -1453032.0
    re = short_compute(data, busi_date, yesterday)
    _re = {
        "orig_trd_amt": "-1439280.0",
        "orig_trd_qty": "-14400",
        "trd_amt": "-1439280.0",
        "weighted_term": "2.0",
        "trd_qty": "-50400",
        "open_type": "trade_exception",
        "close_amt": "1440.0",
        "unclose_qty": "-50400",
        "exception_label": "0",
        "open_date": "2017-03-20",
        "open_timestamp": "1489993201",
        "unclose_amt": "-1453032.0"}
    assert_dict_equal(re["open_detail"][0], _re)


def test_short_compute2():
    """
    考虑在分红分股情况下的，做空异常处理
    """
    busi_date = "2017-03-20"
    yesterday = "2017-03-17"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [
        {"orig_trd_amt": "-1000.0",
         "orig_trd_qty": "-100",
         "trd_amt": "-1000.0",
         "weighted_term": "4.0",
         "trd_qty": "-100",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-100",
         "exception_label": "0",
         "open_date": "2017-01-01",
         "open_timestamp": "1489993201",
         "unclose_amt": "-1000"},
         {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": "3.0",
         "trd_qty": "-600",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-600",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": "-6000"},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "3.0",
         "trd_qty": "-500",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-500",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt":"-5000"}]
    data["now_trd"] = [
        {"timestamp": 1489993205,
         "trd_qty": 0,
         "trd_cash_flow": 700.0,
         "amortize_label": 1},
        {"timestamp": 1489993206,
         "trd_qty": -700,
         "trd_cash_flow": 0.0,
         "amortize_label": 1},
        {"timestamp": 1489993204,
         "trd_qty": 500,
         "trd_cash_flow": 4500.0,
         "amortize_label": 0}]
    data["liab_qty"] = None
    data["mkt_val"] = None
    re = short_compute(data, busi_date, yesterday)
    _re = [
        {"orig_trd_amt": "-1000.0",
         "orig_trd_qty": "-100",
         "trd_amt": "-1000.0",
         "weighted_term": "6.0",
         "trd_qty": "-100",
         "open_type": "trade",
         "close_amt": "900.0",
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-01",
         "open_timestamp": "1489993201",
         "unclose_amt": "0.0"},
         {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": "5.0",
         "trd_qty": "-800",
         "open_type": "trade",
         "close_amt": str(200.0 + 3600 + 2000),
         "unclose_qty": "0",
         "exception_label": "1",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": "0.0"},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "5.0",
         "trd_qty": "-1000",
         "open_type": "trade",
         "close_amt": str(500.0 + 5000),
         "unclose_qty": "0",
         "exception_label": "1",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt": "0.0"}]
    for i in range(len(_re)):
        assert_dict_equal(re["open_detail"][i], _re[i])


def test_short_compute3():
    """
    考虑正常做空情况下分红分股处理
    """
    busi_date = "2017-03-20"
    yesterday = "2017-03-17"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [
        {"orig_trd_amt": "-1000.0",
         "orig_trd_qty": "-100",
         "trd_amt": "-1000.0",
         "weighted_term": "4.0",
         "trd_qty": "-100",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-100",
         "exception_label": "0",
         "open_date": "2017-01-01",
         "open_timestamp": "1489993201",
         "unclose_amt": "-1000"},
         {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": "3.0",
         "trd_qty": "-600",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-600",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": "-6000"},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "3.0",
         "trd_qty": "-500",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-500",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt":"-5000"}]
    data["now_trd"] = [
        {"timestamp": 1489993205,
         "trd_qty": 0,
         "trd_cash_flow": 700.0,
         "amortize_label": 1},
        {"timestamp": 1489993206,
         "trd_qty": -700,
         "trd_cash_flow": 0.0,
         "amortize_label": 1},
        {"timestamp": 1489993204,
         "trd_qty": 500,
         "trd_cash_flow": 4500.0,
         "amortize_label": 0}]
    data["liab_qty"] = -1400
    data["mkt_val"] = -6300.0
    re = short_compute(data, busi_date, yesterday)
    _re = [
        {"orig_trd_amt": "-1000.0",
         "orig_trd_qty": "-100",
         "trd_amt": "-1000.0",
         "weighted_term": "6.0",
         "trd_qty": "-100",
         "open_type": "trade",
         "close_amt": "900.0",
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-01",
         "open_timestamp": "1489993201",
         "unclose_amt": "0.0"},
         {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": str(5.0 + 0.5),
         "trd_qty": "-800",
         "open_type": "trade",
         "close_amt": str(200.0 + 3600),
         "unclose_qty": "-400",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": str(-400 * 4.5)},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "6.0",
         "trd_qty": "-1000",
         "open_type": "trade",
         "close_amt": str(500.0),
         "unclose_qty": "-1000",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt": str(-1000 * 4.5)}]
    for i in range(len(_re)):
        assert_dict_equal(re["open_detail"][i], _re[i])
    # Day 2
    busi_date = "2017-03-21"
    yesterday = "2017-03-20"
    data["now_trd"] = [
         {"timestamp": 1489993208,
         "trd_qty": 1500,
         "trd_cash_flow": 9000.0,
         "amortize_label": 0},
         {"timestamp": 1489993207,
         "trd_qty": -200,
         "trd_cash_flow": -1100.0,
         "amortize_label": 0},
        {"timestamp": 1489993209,
         "trd_qty": 100,
         "trd_cash_flow": 590.0,
         "amortize_label": 0}]
    data["qty"] = None
    data["mkt_val"] = None
    re = short_compute(data, busi_date, yesterday)
    _re = [
        {"orig_trd_amt": "-1000.0",
         "orig_trd_qty": "-100",
         "trd_amt": "-1000.0",
         "weighted_term": "6.0",
         "trd_qty": "-100",
         "open_type": "trade",
         "close_amt": "900.0",
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-01",
         "open_timestamp": "1489993201",
         "unclose_amt": "0.0"},
         {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": str(5.0 + 0.5),
         "trd_qty": "-800",
         "open_type": "trade",
         "close_amt": str(200.0 + 3600 + 400 * 6.0),
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": str(0.0)},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "6.0",
         "trd_qty": "-1000",
         "open_type": "trade",
         "close_amt": str(1000 * 6.0 + 500),
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt": str(0.0)},
        {"orig_trd_amt": "-1100.0",
         "orig_trd_qty": "-200",
         "trd_amt": "-1100.0",
         "weighted_term": "0.0",
         "trd_qty": "-200",
         "open_type": "trade",
         "close_amt": str(100 * 6.0 + 100 * 5.9),
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": busi_date,
         "open_timestamp": "1489993207",
         "unclose_amt": str(0.0)}]
    for i in range(len(_re)):
        assert_dict_equal(re["open_detail"][i], _re[i])


def test_short_compute4():
    """
    考虑在配股情况下，正常做空交易
    """
    busi_date = "2017-03-20"
    yesterday = "2017-03-17"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [
        {"orig_trd_amt": "-1000.0",
         "orig_trd_qty": "-100",
         "trd_amt": "-1000.0",
         "weighted_term": "4.0",
         "trd_qty": "-100",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-100",
         "exception_label": "0",
         "open_date": "2017-01-01",
         "open_timestamp": "1489993201",
         "unclose_amt": "-1000"},
         {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": "3.0",
         "trd_qty": "-600",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-600",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": "-6000"},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "3.0",
         "trd_qty": "-500",
         "open_type": "trade",
         "close_amt": "0.0",
         "unclose_qty": "-500",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt":"-5000"}]
    data["now_trd"] = [
        {"timestamp": 1489993205,
         "trd_qty": 0,
         "trd_cash_flow": 700.0,
         "amortize_label": 1},
        {"timestamp": 1489993206,
         "trd_qty": -700,
         "trd_cash_flow": 700.0,
         "amortize_label": 1},
        {"timestamp": 1489993204,
         "trd_qty": 500,
         "trd_cash_flow": 4500.0,
         "amortize_label": 0}]
    data["liab_qty"] = -1400
    data["mkt_val"] = -6300.0
    re = short_compute(data, busi_date, yesterday)
    _re = [
        {"orig_trd_amt": "-1000.0",
         "orig_trd_qty": "-100",
         "trd_amt": "-1000.0",
         "weighted_term": "6.0",
         "trd_qty": "-100",
         "open_type": "trade",
         "close_amt": "900.0",
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-01",
         "open_timestamp": "1489993201",
         "unclose_amt": "0.0"},
         {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": str(5.0 + 0.5),
         "trd_qty": "-800",
         "open_type": "trade",
         "close_amt": str(200.0 + 3600 + 200.0),
         "unclose_qty": "-400",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": str(-400 * 4.5)},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "6.0",
         "trd_qty": "-1000",
         "open_type": "trade",
         "close_amt": str(500.0 + 500.0),
         "unclose_qty": "-1000",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt": str(-1000 * 4.5)}]
    for i in range(len(_re)):
        assert_dict_equal(re["open_detail"][i], _re[i])


def test_short_trans_close():
    """
    """
    data = {}
    busi_date = "2017-03-20"
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = "2017-03-20"
    data["open_detail"] = [
        {"orig_trd_amt": "-1000.0",
         "orig_trd_qty": "-100",
         "trd_amt": "-1000.0",
         "weighted_term": "6.0",
         "trd_qty": "-100",
         "open_type": "trade",
         "close_amt": "900.0",
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-01",
         "open_timestamp": "1489993201",
         "unclose_amt": "0.0"},
         {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": str(5.0 + 0.5),
         "trd_qty": "-800",
         "open_type": "trade",
         "close_amt": str(200.0 + 3600 + 400 * 6.0),
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": str(0.0)},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "6.0",
         "trd_qty": "-1000",
         "open_type": "trade",
         "close_amt": str(1000 * 6.0 + 500),
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt": str(0.0)},
        {"orig_trd_amt": "-1100.0",
         "orig_trd_qty": "-200",
         "trd_amt": "-1100.0",
         "weighted_term": "0.0",
         "trd_qty": "-200",
         "open_type": "trade",
         "close_amt": str(100 * 6.0 + 100 * 5.9),
         "unclose_qty": "0",
         "exception_label": "0",
         "open_date": busi_date,
         "open_timestamp": "1489993207",
         "unclose_amt": str(0.0)}]
    re = []
    for i in data["open_detail"]:
        re.append(short_trans_close(data, i))
    _re = [
        {"return_rate": 100 / 900.0,
         "weighted_term": 6.0,
         "trd_qty": -100,
         "prd_no": "prd_no",
         "open_type": "trade",
         "close_date": "2017-03-20",
         "open_timestamp": 1489993201,
         "return": 100.0,
         "trade_id": "trade_id",
         "secu_acc_id": "secu_acc_id",
         "trd_amt": -1000.0,
         "busi_date": "2017-03-20",
         "orig_trd_qty": -100,
         "open_date": "2017-01-01",
         "exception_label": 0,
         "close_amt": 900.0,
         "orig_trd_amt": -1000.0}]
    assert_dict_equal(re[0], _re[0])
    assert_less(
        abs(re[1]["return"] - (6000.0 - 9.0 * 400 - 200.0 - 400 * 6)), 1e-4)
    assert_less(
        abs(re[1]["return_rate"] - re[1]["return"] / (9.0 * 400 + 200.0 + 400 * 6)), 1e-4)
    assert_less(
        abs(re[2]["return"] - (5000 - 500 - 1000.0 * 6)), 1e-4)
    assert_less(
        abs(re[2]["return_rate"] - re[2]["return"] / (500 + 1000.0 * 6)), 1e-4)
    assert_less(
        abs(re[3]["return"] - (1100 - 100 * 6.0 - 100 * 5.9)), 1e-4)
    assert_less(
        abs(re[3]["return_rate"] - re[3]["return"] / (100 * (6 + 5.9))), 1e-4)


def test_short_trans_unclose():
    """
    """
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = "2017-03-20"
    data["open_detail"] = [
        {"orig_trd_amt": "-6000.0",
         "orig_trd_qty": "-600",
         "trd_amt": "-6000.0",
         "weighted_term": str(5.0 + 0.5),
         "trd_qty": "-800",
         "open_type": "trade",
         "close_amt": str(200.0 + 3600),
         "unclose_qty": "-400",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993202",
         "unclose_amt": str(-400 * 4.5)},
        {"orig_trd_amt": "-5000.0",
         "orig_trd_qty": "-500",
         "trd_amt": "-5000.0",
         "weighted_term": "6.0",
         "trd_qty": "-1000",
         "open_type": "trade",
         "close_amt": str(500.0),
         "unclose_qty": "-1000",
         "exception_label": "0",
         "open_date": "2017-01-02",
         "open_timestamp": "1489993203",
         "unclose_amt": str(-1000 * 4.5)}]
    re = []
    for i in data["open_detail"]:
        re.append(short_trans_unclose(data, i))
    re = [
        {"unclose_qty": i["unclose_qty"],
         "unclose_amt": i["unclose_amt"],
         "return": i["return"],
         "return_rate": i["return_rate"],
         "close_amt": i["close_amt"]}
        for i in re]
    _re = [
        {"unclose_qty": -400,
         "unclose_amt": -400 * 4.5,
         "close_amt": 400 * 9 + 200,
         "return": -(400 * 4.5 + 200 + 400 * 9 - 6000),
         "return_rate": -(400 * 4.5 + 200 + 400 * 9 - 6000) / (400 * 4.5 + 200 + 400 * 9)},
        {"unclose_qty": -1000,
         "unclose_amt": -1000 * 4.5,
         "close_amt": 500.0,
         "return": -1000 * 4.5 - 500 + 5000,
         "return_rate": (-1000 * 4.5 - 500 + 5000) / (1000 * 4.5 + 500)}]
    assert_dict_equal(re[0], _re[0])
    assert_dict_equal(re[1], _re[1])

