# -*- encoding: UTF-8 -*-
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

from testbase import testBase
from leek.fdata.ctdr.stock_close_tradebyday import StockCloseTradeByDay, \
    _travel_long_iter_row, _travel_short_iter_row
import os
import sys
from leek.common.util import days_between


class SCDHTest(testBase):

    def setUp(self):
        StockCloseTradeByDay.logLevel = 'debug'
        # os.environ[
        #     'SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"
        # sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")
        #self.scdh = StockCloseTradeByDay(None)

    def test_init_data(self):
        self.scdh.init_data('2017-03-16')

    def test_short_compute1(self):
        """
        做空场景1：正常做空交易
        :return:
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        diff = days_between(busi_date, yesterday)
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = [
            {"orig_trd_amt": "-7000.0", "orig_trd_qty": "-700",
             "trd_amt": "-7000.0", "weighted_term": "1.0", "trd_qty": "-700",
             "open_type": "trade_exception", "close_amt": "0.0",
             "unclose_qty": "-700", "exception_label": "0",
             "open_date": "2017-03-17", "unclose_amt": "-7000.0"}]
        data["trd_detail_list"] = [
            {"timestamp": 1490108421, "trd_qty": 350, "trd_cash_flow": 3500.0,
             "amortize_label": 0},
            {"timestamp": 1490108422, "trd_qty": 350, "trd_cash_flow": 3500.0,
             "amortize_label": 0}]
        data["liab_qty"] = -7000
        data["mkt_val"] = -7000
        re = _travel_short_iter_row(data, busi_date, diff)
        print re
        _re = {"orig_trd_amt": "-7000.0", "orig_trd_qty": "-700",
               "trd_amt": "-7000.0", "weighted_term": "3.0", "trd_qty": "-700",
               "open_type": "trade_exception", "close_amt": "7000.0",
               "unclose_qty": "0", "exception_label": "0",
               "open_date": "2017-03-20", "unclose_amt": "0.0"}
        self.assertDictEqual(re["open_detail"][0], _re)

    def test_short_compute2(self):
        """
         做空场景2：分红分股处理正常处理
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = [
            {"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100",
             "trd_amt": "-1000.0", "weighted_term": "4.0", "trd_qty": "-100",
             "open_type": "trade", "close_amt": "0.0", "unclose_qty": "-100",
             "exception_label": "0", "open_date": "2017-01-01",
             "unclose_amt": "-1000"},
            {"orig_trd_amt": "-6000.0", "orig_trd_qty": "-600",
             "trd_amt": "-6000.0", "weighted_term": "3.0", "trd_qty": "-600",
             "open_type": "trade", "close_amt": "0.0", "unclose_qty": "-600",
             "exception_label": "0", "open_date": "2017-01-02",
             "unclose_amt": "-6000"}]
        data["trd_detail_list"] = [
            {"timestamp": 1489993209, "trd_qty": 500, "trd_cash_flow": 4500.0,
             "amortize_label": 0},
            {"timestamp": 1489993205, "trd_qty": 0, "trd_cash_flow": 7000.0,
             "amortize_label": 1},
            {"timestamp": 1489993206, "trd_qty": -700, "trd_cash_flow": 0.0,
             "amortize_label": 1}]
        data["liab_qty"] = -1400
        data["mkt_val"] = -6300.0
        re = _travel_short_iter_row(data, busi_date, 1)
        _re = [{"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100",
                "trd_amt": "-1000.0", "weighted_term": "4.0", "trd_qty": "-200",
                "open_type": "trade", "close_amt": str(1000. + 0.4 * 4500),
                "unclose_qty": str(-100 * 2 + 200), "exception_label": "0",
                "open_date": "2017-01-01", "unclose_amt": "0.0"},
               {"orig_trd_amt": "-6000.0", "orig_trd_qty": "-600",
                "trd_amt": "-6000.0", "weighted_term": "3.75",
                "trd_qty": "-1200", "open_type": "trade",
                "close_amt": str(6000. + 0.6 * 4500),
                "unclose_qty": str(-600 * 2 + 300), "exception_label": "0",
                "open_date": "2017-01-02",
                "unclose_amt": str(-6300 * (900.) / 1400)}]
        for i in range(len(_re)):
            self.assertDictEqual(re["open_detail"][i], _re[i])

    def test_short_compute3(self):
        """
         做空场景3：分红分股处理异常处理
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = [
            {"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100",
             "trd_amt": "-1000.0", "weighted_term": "4.0", "trd_qty": "-100",
             "open_type": "trade", "close_amt": "0.0", "unclose_qty": "-100",
             "exception_label": "0", "open_date": "2017-01-01",
             "unclose_amt": "-1000"},
            {"orig_trd_amt": "-6000.0", "orig_trd_qty": "-600",
             "trd_amt": "-6000.0", "weighted_term": "3.0", "trd_qty": "-600",
             "open_type": "trade", "close_amt": "0.0", "unclose_qty": "-600",
             "exception_label": "0", "open_date": "2017-01-02",
             "unclose_amt": "-6000"}]
        data["trd_detail_list"] = [
            {"timestamp": 1489993209, "trd_qty": 500, "trd_cash_flow": 4500.0,
             "amortize_label": 0},
            {"timestamp": 1489993205, "trd_qty": 0, "trd_cash_flow": 7000.0,
             "amortize_label": 1},
            {"timestamp": 1489993206, "trd_qty": -700, "trd_cash_flow": 0.0,
             "amortize_label": 1}]
        data["liab_qty"] = None
        data["mkt_val"] = None
        re = _travel_short_iter_row(data, busi_date, 1)
        _re = [{"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100",
                "trd_amt": "-1000.0", "weighted_term": "4.0", "trd_qty": "-200",
                "open_type": "trade", "close_amt": str(1000. + 0.4 * 4500),
                "unclose_qty": str(-100 * 2 + 200), "exception_label": "0",
                "open_date": "2017-01-01", "unclose_amt": "0.0"},
               {"orig_trd_amt": "-6000.0", "orig_trd_qty": "-600",
                "trd_amt": "-6000.0", "weighted_term": "3.0",
                "trd_qty": "-1200", "open_type": "trade",
                "close_amt": str(6000. + 0.6 * 4500 + 6000 * 900. / 1200),
                "unclose_qty": str(0), "exception_label": "1",
                "open_date": "2017-01-02",
                "unclose_amt": str(6000 * 900. / 1200 - 6000 * 900. / 1200)}]
        for i in range(len(_re)):
            self.assertDictEqual(re["open_detail"][i], _re[i])

    def test_short_compute4(self):
        """
         做空场景4：配股
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = [
            {"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100",
             "trd_amt": "-1000.0", "weighted_term": "4.0", "trd_qty": "-100",
             "open_type": "trade", "close_amt": "0.0", "unclose_qty": "-100",
             "exception_label": "0", "open_date": "2017-01-01",
             "unclose_amt": "-1000"},
            {"orig_trd_amt": "-6000.0", "orig_trd_qty": "-600",
             "trd_amt": "-6000.0", "weighted_term": "3.0", "trd_qty": "-600",
             "open_type": "trade", "close_amt": "0.0", "unclose_qty": "-600",
             "exception_label": "0", "open_date": "2017-01-02",
             "unclose_amt": "-6000"}]
        data["trd_detail_list"] = [
            {"timestamp": 1489993209, "trd_qty": 500, "trd_cash_flow": 4500.0,
             "amortize_label": 0},
            {"timestamp": 1489993205, "trd_qty": 0, "trd_cash_flow": 7000.0,
             "amortize_label": 1},
            {"timestamp": 1489993206, "trd_qty": -700, "trd_cash_flow": 7000.0,
             "amortize_label": 1}]
        data["liab_qty"] = -1400
        data["mkt_val"] = -6300.0
        re = _travel_short_iter_row(data, busi_date, 1)
        _re = [{"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100",
                "trd_amt": "-1000.0", "weighted_term": "4.0", "trd_qty": "-200",
                "open_type": "trade",
                "close_amt": str(1000. + 0.4 * 4500 + 1000),
                "unclose_qty": str(-100 * 2 + 200), "exception_label": "0",
                "open_date": "2017-01-01", "unclose_amt": "0.0"},
               {"orig_trd_amt": "-6000.0", "orig_trd_qty": "-600",
                "trd_amt": "-6000.0", "weighted_term": "3.75",
                "trd_qty": "-1200", "open_type": "trade",
                "close_amt": str(6000. + 0.6 * 4500 + 6000),
                "unclose_qty": str(-600 * 2 + 300), "exception_label": "0",
                "open_date": "2017-01-02",
                "unclose_amt": str(-6300 * (900.) / 1400)}]
        # print re["open_detail"][1]
        # print _re[1]
        # print re["open_detail"][0]
        # print _re[0]
        for i in range(len(_re)):
            self.assertDictEqual(re["open_detail"][i], _re[i])

    def test_long_compute(self):
        """
        做多场景1：正常情况下普通交易
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = [
            {"orig_trd_amt": "2846482.0", "orig_trd_qty": "296200",
             "trd_amt": "2846482.0", "weighted_term": "2.0",
             "trd_qty": "296200", "close_amt": "0.0", "unclose_qty": "296200",
             "exception_label": "0", "open_date": "2017-03-16",
             "unclose_amt": "2825748.0"},
            {"orig_trd_amt": "958.24", "orig_trd_qty": "100",
             "trd_amt": "958.24", "weighted_term": "1.0", "trd_qty": "100",
             "close_amt": "0.0", "unclose_qty": "100", "exception_label": "0",
             "open_date": "2017-03-17", "unclose_amt": "954.0"}]
        data["trd_detail_list"] = [{"timestamp": 1490004007, "trd_qty": -296300,
                                    "trd_cash_flow": -2828957.76,
                                    "amortize_label": 0}]
        data["qty"] = None
        data["mkt_val"] = None
        re = _travel_long_iter_row(data, busi_date, 1)
        _re = [{"orig_trd_amt": "2846482.0", "orig_trd_qty": "296200",
                "trd_amt": "2846482.0", "weighted_term": "2.0",
                "trd_qty": "296200",
                "close_amt": str(296200 / 296300. * 2828957.76),
                "unclose_qty": "0", "exception_label": "0",
                "open_date": "2017-03-16", "unclose_amt": "0.0"},
               {"orig_trd_amt": "958.24", "orig_trd_qty": "100",
                "trd_amt": "958.24", "weighted_term": "1.0", "trd_qty": "100",
                "close_amt": str(100 / 296300. * 2828957.76),
                "unclose_qty": "0", "exception_label": "0",
                "open_date": "2017-03-17", "unclose_amt": "0.0"}]

        for i in range(len(_re)):
            self.assertDictEqual(re["open_detail"][i], _re[i])

    def test_long_compute2(self):
        """
        做多场景2：正常分红分股情况下的正常交易
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = [{"orig_trd_amt": "1000.0", "orig_trd_qty": "100",
                                "trd_amt": "1000.0", "weighted_term": "4.0",
                                "trd_qty": "100", "open_type": "trade",
                                "close_amt": "0.0", "unclose_qty": "100",
                                "exception_label": "0",
                                "open_date": "2017-01-01",
                                "unclose_amt": "1000"},
                               {"orig_trd_amt": "6000.0", "orig_trd_qty": "600",
                                "trd_amt": "6000.0", "weighted_term": "3.0",
                                "trd_qty": "600", "open_type": "trade",
                                "close_amt": "0.0", "unclose_qty": "600",
                                "exception_label": "0",
                                "open_date": "2017-01-02",
                                "unclose_amt": "6000"}]
        data["trd_detail_list"] = [  # 分股
            {"timestamp": 1489993206, "trd_qty": 700, "trd_cash_flow": 0.0,
             "amortize_label": 1},  # 分红
            {"timestamp": 1489993207, "trd_qty": 0, "trd_cash_flow": -7000.0,
             "amortize_label": 1},  # 交易
            {"timestamp": 1489993208, "trd_qty": -1400,
             "trd_cash_flow": -7000.0, "amortize_label": 0}]
        data["qty"] = 1400
        data["mkt_val"] = 7000
        re = _travel_long_iter_row(data, busi_date, 1)
        _re = [{"orig_trd_amt": "1000.0", "orig_trd_qty": "100",
                "trd_amt": "1000.0", "weighted_term": "4.0", "trd_qty": "200",
                "open_type": "trade", "close_amt": "2000.0", "unclose_qty": "0",
                "exception_label": "0", "open_date": "2017-01-01",
                "unclose_amt": "0.0"},
               {"orig_trd_amt": "6000.0", "orig_trd_qty": "600",
                "trd_amt": "6000.0", "weighted_term": "3.0", "trd_qty": "1200",
                "open_type": "trade", "close_amt": "12000.0",
                "unclose_qty": "0", "exception_label": "0",
                "open_date": "2017-01-02", "unclose_amt": "0.0"}]
        for i in range(len(_re)):
            self.assertDictEqual(re["open_detail"][i], _re[i])

    def test_long_compute3(self):
        """
        做多场景3：正常分红后又分股情况下的异常交易
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = [{"orig_trd_amt": "1000.0", "orig_trd_qty": "100",
                                "trd_amt": "1000.0", "weighted_term": "4.0",
                                "trd_qty": "100", "open_type": "trade",
                                "close_amt": "0.0", "unclose_qty": "100",
                                "exception_label": "0",
                                "open_date": "2017-01-01",
                                "unclose_amt": "1000"},
                               {"orig_trd_amt": "6000.0", "orig_trd_qty": "600",
                                "trd_amt": "6000.0", "weighted_term": "3.0",
                                "trd_qty": "600", "open_type": "trade",
                                "close_amt": "0.0", "unclose_qty": "600",
                                "exception_label": "0",
                                "open_date": "2017-01-02",
                                "unclose_amt": "6000"}]
        data["trd_detail_list"] = [  # 分股
            {"timestamp": 1489993206, "trd_qty": 700, "trd_cash_flow": 0.0,
             "amortize_label": 1},  # 分红
            {"timestamp": 1489993207, "trd_qty": 0, "trd_cash_flow": -7000.0,
             "amortize_label": 1},  # 交易
            {"timestamp": 1489993208, "trd_qty": -400, "trd_cash_flow": -5000.0,
             "amortize_label": 0}]
        data["qty"] = None
        data["mkt_val"] = None
        re = _travel_long_iter_row(data, busi_date, 1)
        _re = [dict(orig_trd_amt="1000.0", orig_trd_qty="100", trd_amt="1000.0",
                    weighted_term="4.0", trd_qty="200", open_type="trade",
                    close_amt=str(1000 - 200.0 / -400 * 5000), unclose_qty="0",
                    exception_label="0", open_date="2017-01-01",
                    unclose_amt="0.0"),
               dict(orig_trd_amt="6000.0", orig_trd_qty="600", trd_amt="6000.0",
                    weighted_term="3.0", trd_qty="1200", open_type="trade",
                    close_amt=str(
                        6000 - 200.0 / -400 * 5000 + 1000.0 / 1200 * 6000),
                    unclose_qty="0", exception_label="1",
                    open_date="2017-01-02", unclose_amt="0.0")]
        for i in range(len(_re)):
            self.assertDictEqual(re["open_detail"][i], _re[i])

    def test_long_compute4(self):
        """
        做多场景4：正常同时分红分股情况下的正常交易
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = [{"orig_trd_amt": "1000.0", "orig_trd_qty": "100",
                                "trd_amt": "1000.0", "weighted_term": "4.0",
                                "trd_qty": "100", "open_type": "trade",
                                "close_amt": "0.0", "unclose_qty": "100",
                                "exception_label": "0",
                                "open_date": "2017-01-01",
                                "unclose_amt": "1000"},
                               {"orig_trd_amt": "6000.0", "orig_trd_qty": "600",
                                "trd_amt": "6000.0", "weighted_term": "3.0",
                                "trd_qty": "600", "open_type": "trade",
                                "close_amt": "0.0", "unclose_qty": "600",
                                "exception_label": "0",
                                "open_date": "2017-01-02",
                                "unclose_amt": "6000"}]
        data["trd_detail_list"] = [  # 分股分红
            {"timestamp": 1489993206, "trd_qty": 700, "trd_cash_flow": 7000.0,
             "amortize_label": 1},  # 交易
            {"timestamp": 1489993208, "trd_qty": -400, "trd_cash_flow": -5000.0,
             "amortize_label": 0}]
        data["qty"] = None
        data["mkt_val"] = None
        re = _travel_long_iter_row(data, busi_date, 1)
        _re = [
            dict(orig_trd_amt="1000.0", orig_trd_qty="100", trd_amt=str(2000.),
                 weighted_term="4.0", trd_qty=str(int(100 + 100. / 700 * 700)),
                 open_type="trade", close_amt=str(-200.0 / -400 * 5000),
                 unclose_qty=str(int(100 + 100. / 700 * 700 - 200)),
                 exception_label="0", open_date="2017-01-01",
                 unclose_amt=str(1000. - 1000.)),
            dict(orig_trd_amt="6000.0", orig_trd_qty="600", trd_amt="12000.0",
                 weighted_term="3.0", trd_qty="1200", open_type="trade",
                 close_amt=str(
                     -200.0 / -400 * 5000 + 6000 * (1200.0 - 200) / (1200)),
                 unclose_qty="0", exception_label="1", open_date="2017-01-02",
                 unclose_amt=str(0.0))]
        for i in range(len(_re)):
            self.assertDictEqual(re["open_detail"][i], _re[i])

    def test_long_compute5(self):
        """
        做多场景5：异常处理，缺失cal数据
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = None
        data["trd_detail_list"] = [
            {"timestamp": 1494241208, "trd_qty": 800, "trd_cash_flow": 5281.32,
             "amortize_label": 0}]
        data["qty"] = None
        data["mkt_val"] = None
        re = _travel_long_iter_row(data, busi_date, 1)
        open_detail = re["open_detail"]
        data["open_detail"] = open_detail
        data["trd_detail_list"] = [
            {"timestamp": 1494327601, "trd_qty": 100, "trd_cash_flow": 645.16,
             "amortize_label": 0},
            {"timestamp": 1494342023, "trd_qty": 270, "trd_cash_flow": 0.,
             "amortize_label": 1}]
        _re = [
            {'open_type': 'trade', 'exception_label': '1', 'trd_amt': '5281.32',
             'close_amt': '5281.32', 'trd_qty': '1040', 'unclose_amt': '0.0',
             'unclose_qty': '0', 'orig_trd_qty': '800',
             'open_date': '2017-03-20', 'weighted_term': '0.0',
             'orig_trd_amt': '5281.32'},
            {'open_type': 'trade', 'exception_label': '1', 'trd_amt': '645.16',
             'close_amt': '645.16', 'trd_qty': '130', 'unclose_amt': '0.0',
             'unclose_qty': '0', 'orig_trd_qty': '100',
             'open_date': '2017-03-21', 'weighted_term': '0.0',
             'orig_trd_amt': '645.16'}]
        re2 = _travel_long_iter_row(data, "2017-03-21", 2)
        for i in range(len(re2['open_detail'])):
            self.assertDictEqual(re["open_detail"][i], _re[i])

    def test_long_compute6(self):
        """
        做多场景6：异常处理，缺失cal数据
        """
        busi_date = "2017-03-20"
        yesterday = "2017-03-17"
        data = {}
        data["trade_id"] = "trade_id"
        data["secu_acc_id"] = "secu_acc_id"
        data["prd_no"] = "prd_no"
        data["busi_date"] = busi_date
        data["open_detail"] = None
        data["trd_detail_list"] = [{"timestamp": 1494241208, "trd_qty": 65400,
                                    "trd_cash_flow": 2733749.27,
                                    "amortize_label": 0},
                                   {"timestamp": 1494241209, "trd_qty": 3700,
                                    "trd_cash_flow": 155401.84,
                                    "amortize_label": 0}]
        data["qty"] = 69100
        data["mkt_val"] = 2867650.0
        re = _travel_long_iter_row(data, busi_date, 1)
        _re = {'open_type': 'trade', 'exception_label': '0',
               'trd_amt': '2889151.11', 'close_amt': '0.0', 'trd_qty': '69100',
               'unclose_amt': '2867650.0', 'unclose_qty': '69100',
               'orig_trd_qty': '69100', 'open_date': '2017-03-20',
               'weighted_term': '1.0', 'orig_trd_amt': '2889151.11'}
        self.assertDictEqual(re['open_detail'][0], _re)

    def all_data_sql(self):
        sql = """
            select * from stock_close_tradebyday_long_data
            where exception_label=0
            and cast(trd_amt+return as bigint)!=cast(close_amt as bigint)
        """
        print sql
        spark = None
        spark.sql("""
            select max(return_rate),min(return_rate),
            abs(datediff(open_date,busi_date)) diffdays
            from test.stock_close_tradebyday_long_data
            where exception_label=0
            group by abs(datediff(open_date,busi_date))
            order by diffdays desc
        """)

        spark.sql("select * from test.stock_close_tradebyday_long_data"
                  " where return_rate='1.622123144891927'").show()
        spark.sql("select * from test.stock_close_tradebyday_long_data"
                  " where return_rate='-0.3446235827905636'").show()
        spark.sql("""
            select max(return_rate),min(return_rate),
            abs(datediff(open_date,busi_date)) diffdays
            from stock_unclose_tradebyday_long_data
            where exception_label=0
            group by abs(datediff(open_date,busi_date))
            order by diffdays desc
        """)

        spark.sql("""
            select max(return_rate),min(return_rate),
            abs(datediff(open_date,close_date)) diffdays
            from test.stock_close_tradebyday_short_data
            where exception_label=0
            group by abs(datediff(open_date,close_date))
            order by diffdays desc
        """)
        spark.sql("""
            select max(return_rate),min(return_rate),
            abs(datediff(open_date,busi_date)) diffdays
            from test.stock_unclose_tradebyday_short_data
            where exception_label=0
            group by abs(datediff(open_date,busi_date))
            order by diffdays desc
        """)


"""
+-------------------+--------------------+--------+
|   max(return_rate)|    min(return_rate)|diffdays|
+-------------------+--------------------+--------+
| 3.3696275071633237| -0.6340526160212829|      25|
|0.15106113935601628|-0.00316597105324...|      24|
| 3.0829986613119145| -0.6644076985632963|      22|
| 2.9100084104289317| -0.6308884913536076|      21|
|         2.63203125| -0.6493911073350326|      20|
|0.17308223727912858|-0.14200554659136752|      19|
|0.25719377835385615|-0.10791532867366693|      18|
|0.19368587213891084|-0.10693393739608022|      17|
|0.23967213114754102| -0.1698826226948971|      16|
|  2.362748643761302| -0.6143302180685358|      15|
| 2.1422777965528894| -0.5757368060315284|      14|
|  2.064601186552406| -0.5333584621183566|      13|
|  2.371283538796229| -0.4867330016583748|      12|
| 2.7088153171120863| -0.4354765161878705|      11|
|0.21771336553945253|-0.19063105592526114|      10|
| 0.1970451147656319|-0.15930194805194806|       9|
|  2.479790419161677| -0.3791374122367101|       8|
|  2.131694173122263|   -0.31715388858246|       7|
|  2.225112729795352| -0.2487864077669903|       6|
| 0.2186389029964449|-0.17356475300400534|       5|
+-------------------+--------------------+--------+
"""


def test_long_compute4():
    """
     做空场景4：配股
    """
    busi_date = "2017-03-20"
    yesterday = "2017-03-17"
    data = {}
    data["trade_id"] = "trade_id"
    data["secu_acc_id"] = "secu_acc_id"
    data["prd_no"] = "prd_no"
    data["busi_date"] = busi_date
    data["open_detail"] = None
    data["trd_detail_list"] = [
        {"timestamp": 1489993209, "trd_qty": 200, "trd_cash_flow": 6928.97, "amortize_label": 0},
        {"timestamp": 1489993205, "trd_qty": 200, "trd_cash_flow": 6866.96, "amortize_label": 0},
        {"timestamp": 1489993206, "trd_qty": -200, "trd_cash_flow": -6926.000000000001, "amortize_label": 0}]
    data["qty"] = 200
    data["mkt_val"] = 6926.000000000001
    re = _travel_long_iter_row(data, busi_date, 1)
    print re
    # _re = [{"orig_trd_amt": "-1000.0", "orig_trd_qty": "-100", "trd_amt": "-1000.0",
    #         "weighted_term": "4.0", "trd_qty": "-200", "open_type": "trade",
    #         "close_amt": str(1000. + 0.4 * 4500 + 1000), "unclose_qty": str(-100 * 2 + 200),
    #         "exception_label": "0", "open_date": "2017-01-01", "unclose_amt": "0.0"},
    #        {"orig_trd_amt": "-6000.0", "orig_trd_qty": "-600", "trd_amt": "-6000.0",
    #         "weighted_term": "3.75", "trd_qty": "-1200", "open_type": "trade",
    #         "close_amt": str(6000. + 0.6 * 4500 + 6000), "unclose_qty": str(-600 * 2 + 300),
    #         "exception_label": "0", "open_date": "2017-01-02",
    #         "unclose_amt": str(-6300 * (900.) / 1400)}]
    # # print re["open_detail"][1]
    # # print _re[1]
    # # print re["open_detail"][0]
    # # print _re[0]
    # for i in range(len(_re)):
    #     self.assertDictEqual(re["open_detail"][i], _re[i])

{'open_type': 'trade', 'exception_label': '0', 'trd_amt': '13795.93', 'close_amt': '6926.0', 'trd_qty': '400', 'unclose_amt': '6928.97', 'unclose_qty': '200', 'orig_trd_qty': '400', 'open_date': '2017-03-20', 'weighted_term': '0.5', 'orig_trd_amt': '13795.93'},\
{'open_type': 'trade_exception', 'exception_label': '1', 'trd_amt': '6926.0', 'close_amt': '0.0', 'trd_qty': '200', 'unclose_amt': '6926.0', 'unclose_qty': '200', 'orig_trd_qty': '200', 'open_date': '2017-03-20', 'weighted_term': '1.0', 'orig_trd_amt': '6926.0'}


[{'open_type': 'trade', 'exception_label': '0', 'trd_amt': '13795.93', 'close_amt': '6926.0', 'trd_qty': '400', 'unclose_amt': '6928.97', 'unclose_qty': '200', 'orig_trd_qty': '400', 'open_date': '2017-03-20', 'weighted_term': '0.5', 'orig_trd_amt': '13795.93'},


 {'open_type': 'trade_exception', 'exception_label': '1', 'trd_amt': '6926.0', 'close_amt': '0.0', 'trd_qty': '200', 'unclose_amt': '6926.0', 'unclose_qty': '200', 'orig_trd_qty': '200', 'open_date': '2017-03-20', 'weighted_term': '1.0', 'orig_trd_amt': '6926.0'}],