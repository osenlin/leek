# -*- coding: utf-8 -*-


from leek.fdata.cctr.tests.testing import assert_less, assert_equal, assert_array_almost_equal, \
    assert_true, assert_dict_equal
from leek.fdata.cctr.long_close_return import long_compute


def test_long_compute():
    """
    考虑正常情况下普通交易
    """
    busi_date = "2017-03-20"
    yesterday = "2017-03-17"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [
        {"orig_trd_amt": "2846482.0", "orig_trd_qty": "296200", "trd_amt": "2846482.0",
         "unclose_inv": "2846482.0", "weighted_term": "1.0", "gain_return": "0.0",
         "trd_qty": "296200", "close_amt": "0.0", "unclose_qty": "296200", "exception_label": "0",
         "open_date": "2017-03-16", "open_timestamp": "1489626000", "unclose_amt": "2825748.0"},
        {"orig_trd_amt": "958.24", "orig_trd_qty": "100", "trd_amt": "958.24", "gain_return": "0.0",
         "unclose_inv": "958.24", "weighted_term": "1.0", "trd_qty": "100", "close_amt": "0.0",
         "unclose_qty": "100", "exception_label": "0", "open_date": "2017-03-17",
         "open_timestamp": "1489748407", "unclose_amt": "954.0"}]
    data["now_trd"] = [{"timestamp": 1490004007, "trd_qty": -296300, "trd_cash_flow": -2828957.76,
                        "amortize_label": 0}]
    data["qty"] = None
    data["mkt_val"] = None
    re = long_compute(data, busi_date, yesterday)
    cal_return = int(sum([item.get('return') for item in re['close_detail']]))
    exp_return = int(
        2828957.76 - sum([float(item.get('orig_trd_amt')) for item in data['open_detail']]))
    assert_equal(cal_return, exp_return)
    exp_return_rate = (2828957.76 - 958.24 - 2846482) / (958.24 + 2846482)
    cal_return_rate = re['close_detail'][0].get('return_rate')
    assert_less(abs(exp_return_rate - cal_return_rate), 1e-4)


def test_long_compute2():
    """
    考虑在分红分股情况下的，做多异常处理
    """
    busi_date = "2017-01-04"
    yesterday = "2017-01-03"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [{"orig_trd_amt": "1000.0", "orig_trd_qty": "100", "trd_amt": "1000.0",
                            "unclose_inv": "1000.0", "gain_return": "0.0", "weighted_term": "4.0",
                            "trd_qty": "100", "open_type": "trade", "close_amt": "0.0",
                            "unclose_qty": "100", "exception_label": "0", "open_date": "2017-01-01",
                            "open_timestamp": "1489993201", "unclose_amt": "1000"},
        {"orig_trd_amt": "6000.0", "orig_trd_qty": "600", "trd_amt": "6000.0",
         "unclose_inv": "6000.0", "weighted_term": "3.0", "trd_qty": "600", "gain_return": "0.0",
         "open_type": "trade", "close_amt": "0.0", "unclose_qty": "600", "exception_label": "0",
         "open_date": "2017-01-02", "open_timestamp": "1489993202", "unclose_amt": "6000"},
        {"orig_trd_amt": "5000.0", "orig_trd_qty": "500", "trd_amt": "5000.0", "gain_return": "0.0",
         "unclose_inv": "5000.0", "weighted_term": "3.0", "trd_qty": "500", "open_type": "trade",
         "close_amt": "0.0", "unclose_qty": "500", "exception_label": "0",
         "open_date": "2017-01-02", "open_timestamp": "1489993203", "unclose_amt": "5000"}]
    data["now_trd"] = [
        {"timestamp": 1489993205, "trd_qty": 0, "trd_cash_flow": -700.0, "amortize_label": 1},
        {"timestamp": 1489993206, "trd_qty": 700, "trd_cash_flow": 0.0, "amortize_label": 1},
        {"timestamp": 1489993204, "trd_qty": -500, "trd_cash_flow": -4500.0, "amortize_label": 0}]
    data["qty"] = None
    data["mkt_val"] = None
    re = long_compute(data, busi_date, yesterday)
    exp_result = [
        {'close_inv': '5000.0', 'weighted_term': '3.2', 'return': '-500.0', 'open_qty': '-500',
         'exception_label': '0', 'return_rate': '-0.1', 'unclose_amt': '0.0',
         'close_date': '2017-01-04', 'close_type': 'trade', 'close_amt': '4500.0',
         'close_timestamp': '1489993204', 'open_amt': '-4500.0', 'close_qty': '500',
         'open_date': '2017-01-04', 'unclose_qty': '0'},
        {'return': '500.0', 'open_qty': '1000', 'weighted_term': '3.0', 'return_rate': '0.1',
         'unclose_amt': '0.0', 'exception_label': '1', 'close_date': '2017-01-04',
         'open_amt': '5500.0', 'close_type': 'trade_exception', 'close_amt': '5500.0',
         'close_timestamp': '1537860183324', 'open_date': '2017-01-02', 'close_qty': '1000',
         'close_inv': '5000.0', 'unclose_qty': '0'},
        {'return': '200.0', 'open_qty': '400', 'weighted_term': '3.0', 'return_rate': '0.1',
         'unclose_amt': '0.0', 'exception_label': '1', 'close_date': '2017-01-04',
         'open_amt': '2200.0', 'close_type': 'trade_exception', 'close_amt': '2200.0',
         'close_timestamp': '1537860183324', 'open_date': '2017-01-02', 'close_qty': '400',
         'close_inv': '2000.0', 'unclose_qty': '0'}]
    for i in xrange(0, 3):
        assert_equal(re['close_detail'][i]['return'], exp_result[i]['return'])
        assert_equal(re['close_detail'][i]['weighted_term'], exp_result[i]['weighted_term'])


def test_long_compute3():
    """
    考虑在配股,分红，分股情况下，正常做多交易
    """
    busi_date = "2017-01-04"
    yesterday = "2017-01-03"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [{"orig_trd_amt": "1000.0", "orig_trd_qty": "100", "trd_amt": "1000.0",
                            "unclose_inv": "1000.0", "weighted_term": "3.0", "trd_qty": "100",
                            "open_type": "trade", "gain_return": "0.0", "close_amt": "0.0",
                            "unclose_qty": "100", "exception_label": "0", "open_date": "2017-01-01",
                            "open_timestamp": "1489993201", "unclose_amt": "1000"},
        {"orig_trd_amt": "6600.0", "orig_trd_qty": "600", "trd_amt": "6600.0",
         "unclose_inv": "6600.0", "weighted_term": "2.0", "trd_qty": "600", "gain_return": "0.0",
         "open_type": "trade", "close_amt": "0.0", "unclose_qty": "600", "exception_label": "0",
         "open_date": "2017-01-02", "open_timestamp": "1489993202", "unclose_amt": "6600"},
        {"orig_trd_amt": "5000.0", "orig_trd_qty": "500", "trd_amt": "5000.0",
         "unclose_inv": "5000.0", "weighted_term": "2.0", "trd_qty": "500", "gain_return": "0.0",
         "open_type": "trade", "close_amt": "0.0", "unclose_qty": "500", "exception_label": "0",
         "open_date": "2017-01-02", "open_timestamp": "1489993203", "unclose_amt": "5000"}]
    data["now_trd"] = [
        {"timestamp": 1489993204, "trd_qty": -500, "trd_cash_flow": -4500.0, "amortize_label": 0},
        {"timestamp": 1489993205, "trd_qty": 0, "trd_cash_flow": -700.0, "amortize_label": 1},
        {"timestamp": 1489993206, "trd_qty": 700, "trd_cash_flow": 1400.0, "amortize_label": 1},
        {"timestamp": 1489993207, "trd_qty": 200, "trd_cash_flow": 1100.0, "amortize_label": 0},
        {"timestamp": 1489993208, "trd_qty": -1500, "trd_cash_flow": -9000.0, "amortize_label": 0},
        {"timestamp": 1489993209, "trd_qty": -100, "trd_cash_flow": -590.0, "amortize_label": 0}, ]
    data["qty"] = None
    data["mkt_val"] = None
    re = long_compute(data, busi_date, yesterday)
    print re['close_detail']
    print re['open_detail']
    rel_return = sum([float(item['return']) for item in re['close_detail']])
    exp_return = sum(
        [float(item['close_amt']) + float(item['unclose_amt']) - float(item['trd_amt']) for item in
         re['open_detail']])
    #print re
    assert_equal(rel_return, exp_return)


def test_long_compute_4():
    """
    在没有交易流水情况下的处理
    """
    busi_date = "2017-03-21"
    yesterday = "2017-03-20"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [
        {"orig_trd_amt": "4000", "orig_trd_qty": "500", "trd_amt": "4000", "gain_return": "0.0",
         "unclose_inv": "4000", "weighted_term": "1.0", "trd_qty": "500", "close_amt": "0.0",
         "unclose_qty": "500", "exception_label": "0", "open_date": "2017-03-16",
         "open_timestamp": "1489748407", "unclose_amt": "4000"}]
    data["now_trd"] = None
    data["qty"] = 400
    data["mkt_val"] = 3600
    re = long_compute(data, busi_date, yesterday)
    assert_equal(re['close_detail'][0]['return_rate'], '0.5')
    assert_equal(re['close_detail'][0]['weighted_term'], '0.2')


def test_long_compute_ex():
    """
    考虑正常情况下普通交易
    """
    busi_date = "2017-03-20"
    yesterday = "2017-03-17"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [
        {"orig_trd_qty": "1600", "trd_amt": "18068.52", "weighted_term": "4.0", "close_amt": "0.0",
         "exception_label": "0", "open_date": "2017-03-21", "unclose_amt": "17232.0",
         "orig_trd_amt": "18068.52", "gain_return": "0.0", "trd_qty": "1600",
         "unclose_inv": "18068.52", "open_type": "trade", "unclose_qty": "1600",
         "open_timestamp": "1490094000"},
        {"orig_trd_qty": "1500", "trd_amt": "16845.0", "weighted_term": "4.0", "close_amt": "0.0",
         "exception_label": "1", "open_date": "2017-03-21", "unclose_amt": "16155.0",
         "orig_trd_amt": "16845.0", "gain_return": "0.0", "trd_qty": "1500", "unclose_inv": "0.0",
         "open_type": "trade_exception", "unclose_qty": "1500", "open_timestamp": "1490079601"}]
    data["now_trd"] = [{"timestamp": 1490004007, "trd_qty": -3100, "trd_cash_flow": -32603.85,
                        "amortize_label": 0}]
    data["qty"] = None
    data["mkt_val"] = None
    re = long_compute(data, busi_date, yesterday)
    print re['close_detail']
    print re['open_detail']


def test_all_data():
    sql1 = """
        select max(return_rate), min(return_rate),
        max(weighted_term), min(weighted_term)
        from  fdata.stock_close_c_trade_long_data
    """
    print sql1
    sql2 = """
        select busi_date, exception_label, max(return), min(return),
        max(weighted_term), min(weighted_term)
        from  fdata.stock_close_c_trade_long_data
        group by busi_date, exception_label
        order by busi_date, exception_label
    """
    print sql2
    sql3 = """
        select * from odata.stock_cash_flow_detail
        where trade_id='21997' and
          prd_no='2.300533'  and busi_date<='2017-03-17';
    """
    print sql3
    sql4 = """
        select * from odata.stock_debt_holding
        where trade_id='21997'
          and prd_no='2.300533'  and busi_date<='2017-03-17'
        order by busi_date asc;
    """
    print sql4
    sql5 = """
        select * from  fdata.stock_close_c_trade_long_data
        where return<0 and return_rate>0
    """
    print sql5


def test_long_compute3():
    """
    考虑在配股,分红，分股情况下，正常做多交易
    """
    busi_date = "2017-01-04"
    yesterday = "2017-01-03"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = [{"orig_trd_amt": "19726.31", "orig_trd_qty": "1000", "trd_amt": "19726.31",
                            "unclose_inv": "19726.31", "weighted_term": "3.0", "trd_qty": "1000",
                            "open_type": "trade", "gain_return": "0.0", "close_amt": "0.0",
                            "unclose_qty": "1000", "exception_label": "0", "open_date": "2017-01-01",
                            "open_timestamp": "1489993201", "unclose_amt": "19726.31"}]
    data["now_trd"] = [
        {"timestamp": 1489993203, "trd_qty": 1000, "trd_cash_flow": 18806.02, "amortize_label": 0},
        {"timestamp": 1489993204, "trd_qty": -500, "trd_cash_flow": -9470.32, "amortize_label": 0},
        {"timestamp": 1489993205, "trd_qty": 0, "trd_cash_flow": -126.0, "amortize_label": 1},
        {"timestamp": 1489993206, "trd_qty": 1500, "trd_cash_flow": 0.0, "amortize_label": 1},
        {"timestamp": 1489993207, "trd_qty": -1500, "trd_cash_flow": -13735.96, "amortize_label": 0},
        {"timestamp": 1489993208, "trd_qty": -1000, "trd_cash_flow": -8706.11, "amortize_label": 0},
        {"timestamp": 1489993209, "trd_qty": -300, "trd_cash_flow": -2692.25, "amortize_label": 0},
        {"timestamp": 1489993210, "trd_qty": -100, "trd_cash_flow": -894.08, "amortize_label": 0},
        {"timestamp": 1489993211, "trd_qty": -100, "trd_cash_flow":  -869.1, "amortize_label": 0},
    ]
    data["qty"] = None
    data["mkt_val"] = None
    re = long_compute(data, busi_date, yesterday)
    print re['close_detail']
    print re['open_detail']
    rel_return = sum([float(item['return']) for item in re['close_detail']])
    exp_return = sum(
        [float(item['close_amt']) + float(item['unclose_amt']) - float(item['trd_amt']) for item in
         re['open_detail']])
    print rel_return
    assert_equal(abs(rel_return-exp_return) > 0.0001, False)