# -*- coding: utf-8 -*-


from leek.fdata.cctr.tests.testing import assert_less, assert_array_almost_equal, \
    assert_dict_equal, assert_equal
from leek.fdata.cctr.short_close_return import short_compute


def test_short_compute_1():
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
        {"orig_trd_amt": "-1439280.0", "orig_trd_qty": "-14400", "trd_amt": "-1439280.0",
         "weighted_term": "1.0", "gain_return": 0, "unclose_inv": "-1439280.0", "trd_qty": "-14400",
         "open_type": "trade_exception", "close_amt": "0.0", "unclose_qty": "-14400",
         "exception_label": "0", "open_date": "2017-03-20", "open_timestamp": "1489993201",
         "unclose_amt": "-1439280.0"}]
    data["now_trd"] = [
        {"timestamp": 1490108421, "trd_qty": -36000, "trd_cash_flow": 0.0, "amortize_label": 1},
        {"timestamp": 1490108420, "trd_qty": 0, "trd_cash_flow": 1440.0, "amortize_label": 1}]
    data["liab_qty"] = -50400
    data["mkt_val"] = -1453032.0
    re = short_compute(data, busi_date, yesterday)
    assert_equal(re['close_detail'], [])


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
    data["open_detail"] = [{"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100", "trd_amt": "-1000.0",
                            "unclose_inv": "-1000.0", "weighted_term": "4.0", "trd_qty": "-100",
                            "open_type": "trade", "gain_return": "0", "close_amt": "0.0",
                            "unclose_qty": "-100", "exception_label": "0",
                            "open_date": "2017-01-01", "open_timestamp": "1489993201",
                            "unclose_amt": "-1000"},
                           {"orig_trd_amt": "-6000.0", "orig_trd_qty": "-600", "trd_amt": "-6000.0",
                            "unclose_inv": "-6000.0", "weighted_term": "3.0", "trd_qty": "-600",
                            "gain_return": "0", "open_type": "trade", "close_amt": "0.0",
                            "unclose_qty": "-600", "exception_label": "0",
                            "open_date": "2017-01-02", "open_timestamp": "1489993202",
                            "unclose_amt": "-6000"},
                           {"orig_trd_amt": "-5000.0", "orig_trd_qty": "-500", "trd_amt": "-5000.0",
                            "gain_return": "0", "unclose_inv": "-5000.0", "weighted_term": "3.0",
                            "trd_qty": "-500", "open_type": "trade", "close_amt": "0.0",
                            "unclose_qty": "-500", "exception_label": "0",
                            "open_date": "2017-01-02", "open_timestamp": "1489993203",
                            "unclose_amt": "-5000"}]
    data["now_trd"] = [
        {"timestamp": 1489993205, "trd_qty": 0, "trd_cash_flow": 700.0, "amortize_label": 1},
        {"timestamp": 1489993206, "trd_qty": -700, "trd_cash_flow": 0.0, "amortize_label": 1},
        {"timestamp": 1489993204, "trd_qty": 500, "trd_cash_flow": 4500.0, "amortize_label": 0}]
    data["liab_qty"] = None
    data["mkt_val"] = None
    re = short_compute(data, busi_date, yesterday)
    exp_result = [
        {'close_inv': '5000.0', 'weighted_term': '2.8', 'return': '500.0', 'open_qty': '500',
         'exception_label': '0', 'return_rate': '0.111111111111', 'unclose_amt': '0.0',
         'close_date': '2017-03-20', 'close_type': 'trade', 'close_amt': '4500.0',
         'close_timestamp': '1489993204', 'open_amt': '4500.0', 'close_qty': '500',
         'open_date': '2017-03-20', 'unclose_qty': '0'},
        {'close_inv': '5000.0', 'return': '-500.0', 'close_type': 'trade_exception',
         'exception_label': '1', 'return_rate': '-0.0909090909091', 'unclose_amt': '0.0',
         'unclose_qty': '0', 'close_amt': '5000.0', 'close_timestamp': '1537863637774',
         'close_qty': '1000.0', 'weighted_term': '5.0', 'close_date': '2017-03-20'},
        {'close_inv': '2000.0', 'return': '-200.0', 'close_type': 'trade_exception',
         'exception_label': '1', 'return_rate': '-0.0909090909091', 'unclose_amt': '0.0',
         'unclose_qty': '0', 'close_amt': '2000.0', 'close_timestamp': '1537863637774',
         'close_qty': '400.0', 'weighted_term': '5.0', 'close_date': '2017-03-20'}]
    for i in xrange(len(exp_result)):
        assert_equal(re['close_detail'][i]['return'], exp_result[i]['return'])


def test_short_compute3():
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
    data["open_detail"] = [{"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100", "trd_amt": "-1000.0",
                            "unclose_inv": "-1000.0", "weighted_term": "4.0", "trd_qty": "-100",
                            "open_type": "trade", "gain_return": "0", "close_amt": "0.0",
                            "unclose_qty": "-100", "exception_label": "0",
                            "open_date": "2017-01-01", "open_timestamp": "1489993201",
                            "unclose_amt": "-1000"},
                           {"orig_trd_amt": "-6600.0", "orig_trd_qty": "-600", "trd_amt": "-6000.0",
                            "unclose_inv": "-6600.0", "weighted_term": "3.0", "trd_qty": "-600",
                            "gain_return": "0", "open_type": "trade", "close_amt": "0.0",
                            "unclose_qty": "-600", "exception_label": "0",
                            "open_date": "2017-01-02", "open_timestamp": "1489993202",
                            "unclose_amt": "-6600"},
                           {"orig_trd_amt": "-5000.0", "orig_trd_qty": "-500", "trd_amt": "-5000.0",
                            "gain_return": "0", "unclose_inv": "-5000.0", "weighted_term": "3.0",
                            "trd_qty": "-500", "open_type": "trade", "close_amt": "0.0",
                            "unclose_qty": "-500", "exception_label": "0",
                            "open_date": "2017-01-02", "open_timestamp": "1489993203",
                            "unclose_amt": "-5000"}]
    data["now_trd"] = [
        {"timestamp": 1489993204, "trd_qty": 500, "trd_cash_flow": 4500.0, "amortize_label": 0},
        {"timestamp": 1489993205, "trd_qty": 0, "trd_cash_flow": 700.0, "amortize_label": 1},
        {"timestamp": 1489993206, "trd_qty": -700, "trd_cash_flow": 1400, "amortize_label": 1},
        {"timestamp": 1489993207, "trd_qty": -200, "trd_cash_flow": -1100, "amortize_label": 0},
        {"timestamp": 1489993208, "trd_qty": 1500, "trd_cash_flow": 9000, "amortize_label": 0},
        {"timestamp": 1489993209, "trd_qty": 100, "trd_cash_flow": 590, "amortize_label": 0}, ]
    data["liab_qty"] = None
    data["mkt_val"] = None
    re = short_compute(data, busi_date, yesterday)
    print re['close_detail']
    exp_result = [
        {'close_inv': '5400.0', 'weighted_term': '2.8', 'return': '900.0', 'open_qty': '500',
         'exception_label': '0', 'return_rate': '0.2', 'unclose_amt': '0.0',
         'close_date': '2017-03-20', 'close_type': 'trade', 'close_amt': '4500.0',
         'close_timestamp': '1489993204', 'open_amt': '4500.0', 'close_qty': '500',
         'open_date': '2017-03-20', 'unclose_qty': '0'},
        {'close_inv': '7750.0', 'weighted_term': '-4.66666666667', 'return': '-3350.0',
         'open_qty': '1500', 'exception_label': '0', 'return_rate': '-0.301801801802',
         'unclose_amt': '0.0', 'close_date': '2017-03-20', 'close_type': 'trade',
         'close_amt': '11100.0', 'close_timestamp': '1489993208', 'open_amt': '9000',
         'close_qty': '1500', 'open_date': '2017-03-20', 'unclose_qty': '0'},
        {'close_inv': '550.0', 'weighted_term': '0.0', 'return': '-40.0', 'open_qty': '100',
         'exception_label': '0', 'return_rate': '-0.0677966101695', 'unclose_amt': '0.0',
         'close_date': '2017-03-20', 'close_type': 'trade', 'close_amt': '590.0',
         'close_timestamp': '1489993209', 'open_amt': '590', 'close_qty': '100',
         'open_date': '2017-03-20', 'unclose_qty': '0'}]
    for i in xrange(len(exp_result)):
        assert_equal(re['close_detail'][i]['return'], exp_result[i]['return'])


def test_short_compute_4():
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
        {"orig_trd_amt": "-5000", "orig_trd_qty": "-500", "trd_amt": "-5000", "gain_return": "0.0",
         "unclose_inv": "-5000", "weighted_term": "1.0", "trd_qty": "-500", "close_amt": "0.0",
         "unclose_qty": "-500", "exception_label": "0", "open_date": "2017-03-16",
         "open_timestamp": "1489748407", "unclose_amt": "-5000"}]
    data["now_trd"] = None
    data["liab_qty"] = -400
    data["mkt_val"] = -3200
    re = short_compute(data, busi_date, yesterday)
    assert_equal(re['close_detail'][0]['return_rate'], '0.8')
    assert_equal(re['close_detail'][0]['weighted_term'], '0.2')


def test_short_compute_5():
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
        {"orig_trd_amt": "-553735.57", "orig_trd_qty": "-169900", "trd_amt": "-553735.57",
         "weighted_term": "5.0", "gain_return": 0, "unclose_inv": "-553735.57",
         "trd_qty": "-169900", "open_type": "trade_exception", "close_amt": "0.0",
         "unclose_qty": "-169900", "exception_label": "0", "open_date": "2017-03-20",
         "open_timestamp": "1489993201", "unclose_amt": "-553735.57"}]
    data["now_trd"] = [
        {"timestamp": 1490108421, "trd_qty": -24800, "trd_cash_flow": -82336, "amortize_label": 0},
        {"timestamp": 1490108425, "trd_qty": 194700, "trd_cash_flow": 648513.09,
         "amortize_label": 0}]
    data["liab_qty"] = None
    data["mkt_val"] = None
    re = short_compute(data, busi_date, yesterday)
    print re['close_detail']


def test_all_data():
    sql1 = """
        select max(return_rate), min(return_rate),
        max(weighted_term), min(weighted_term)
        from  fdata.stock_close_c_trade_short_data
    """
    print sql1
    sql2 = """
        select busi_date, exception_label, max(return),
        min(return), max(weighted_term), min(weighted_term)
        from  stock_close_c_trade_short_data
        group by busi_date, exception_label
        order by busi_date, exception_label
    """
    print sql2
    sql3 = """
        select * from odata.stock_cash_flow_detail
        where trade_id='21997' and prd_no='2.300533'
         and busi_date<='2017-03-17'
    """
    print sql3
    sql4 = """
        select * from odata.stock_debt_holding
        where trade_id='21997' and prd_no='2.300533' and busi_date<='2017-03-17'
        order by busi_date asc
    """
    print sql4
    sql5 = """
        select * from   stock_close_c_trade_short_data
        where return<0 and return_rate>0
    """
    print sql5
