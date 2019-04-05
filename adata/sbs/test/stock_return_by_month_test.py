# coding=utf-8
from testbase import testBase
from pyspark.sql import Row, functions as F
from leek.adata.sbs.stock_return_by_month import StockReturnByMonth, _travel_row2


class SCDHTest(testBase):

    def setUp(self):
        StockReturnByMonth.logLevel = 'debug'
        self.scdh = StockReturnByMonth(None)
        self.spark = testBase.setupSpark(None, "yarn", "app_cash_flow_merge")

    def test_get_base_data(self):
        self.scdh._get_base_data("2017-03-16", "2017-03-18")

    def test_init_data(self):
        self.scdh.init_data()

    def test_daily_compute(self):
        self.scdh.daily_compute("2017-03-16", "2017-03-16")

    def test_check_1(self):
        sql = """
            SELECT * from adatatest.stock_cust_daily_return
            where short_return_rate>1 or long_return_rate>1 or total_return_rate>1
        """
        self.spark.sql(sql)

    def test_check_rate(self):
        row = Row(trade_id=u'10096', detail_list=[
            {u'long_return': u'0.0', u'pre_asset_val': u'7217500.0', u'now_debt_val': u'0.0',
             u'capital_out': u'0.0', u'capital_in': u'7254796.91', u'short_return': u'0.0',
             u'exception_label': u'0', u'total_return': u'7254796.91', u'pre_debt_val': u'0.0',
             u'busi_date': u'2017-03-16', u'now_asset_val': u'7254796.91'},
            {u'long_return': u'0.0', u'pre_asset_val': u'7254796.91', u'now_debt_val': u'0.0',
             u'capital_out': u'0.0', u'capital_in': u'7161557.15',
             u'short_return': u'225.65000000000055', u'exception_label': u'0',
             u'total_return': u'7161557.15', u'pre_debt_val': u'0.0', u'busi_date': u'2017-03-18',
             u'now_asset_val': u'7161557.15'},
            {u'long_return': u'0.0', u'pre_asset_val': u'7254796.91', u'now_debt_val': u'0.0',
             u'capital_out': u'0.0', u'capital_in': u'7161557.15',
             u'short_return': u'225.65000000000055', u'exception_label': u'0',
             u'total_return': u'7161557.15', u'pre_debt_val': u'0.0', u'busi_date': u'2017-03-17',
             u'now_asset_val': u'7161557.15'}])
        a1 = _travel_row2(row, '2017-03-18')['total_return_rate']
        a2 = (7254796.91 + 7161557.15 + 7161557.15) / (
            7217500.0 + 7254796.91 + 7161557.15 + 7161557.15)
        self.assertEquals(a1, a2, "收益率比对")

    def test_check_67766(self):
        """
        # spark.sql("""
        #                   select  trade_id,collect_list(detail_item) detail_list from (
        #                     select trade_id,
        #                         (str_to_map(concat(
        #                         'busi_date:',busi_date,
        #                         ',pre_asset_val:',pre_asset_val,
        #                         ',pre_debt_val:',pre_debt_val,
        #                         ',now_asset_val:',now_asset_val,
        #                         ',now_debt_val:',now_debt_val,
        #                         ',capital_in:',capital_in,
        #                         ',capital_out:',capital_out,
        #                         ',exception_label:',exception_label,
        #                         ',long_return:',long_return,
        #                         ',total_return:',total_return,
        #                         ',short_return:',short_return
        #                         ),",",":")) detail_item
        #                     from adatatest.stock_cust_daily_return
        #                     where trade_id='1987' and busi_date<='2017-03-23'
        #                   )  a
        #                   group by trade_id
        #                 """)
        #
        # """
        r = Row(trade_id=u'1987', detail_list=[
            {u'long_return': u'-101388.37999999974', u'pre_asset_val': u'6559961.74',
             u'now_debt_val': u'0.0', u'capital_out': u'0.0', u'capital_in': u'0.0',
             u'short_return': u'0.0', u'exception_label': u'0',
             u'total_return': u'-101388.37999999974', u'pre_debt_val': u'0.0',
             u'busi_date': u'2017-03-22', u'now_asset_val': u'6458573.36'},
            {u'long_return': u'-110096.59', u'pre_asset_val': u'6670058.33',
             u'now_debt_val': u'0.0', u'capital_out': u'0.0', u'capital_in': u'0.0',
             u'short_return': u'0.0', u'exception_label': u'0', u'total_return': u'-110096.59',
             u'pre_debt_val': u'0.0', u'busi_date': u'2017-03-21', u'now_asset_val': u'6559961.74'},
            {u'long_return': u'-35803.0', u'pre_asset_val': u'6705861.33', u'now_debt_val': u'0.0',
             u'capital_out': u'0.0', u'capital_in': u'0.0', u'short_return': u'0.0',
             u'exception_label': u'0', u'total_return': u'-35803.0', u'pre_debt_val': u'0.0',
             u'busi_date': u'2017-03-20', u'now_asset_val': u'6670058.33'},
            {u'long_return': u'205659.0', u'pre_asset_val': u'6500202.33', u'now_debt_val': u'0.0',
             u'capital_out': u'0.0', u'capital_in': u'0.0', u'short_return': u'0.0',
             u'exception_label': u'0', u'total_return': u'205659.0', u'pre_debt_val': u'0.0',
             u'busi_date': u'2017-03-17', u'now_asset_val': u'6705861.33'},
            {u'long_return': u'-8797.670000000113', u'pre_asset_val': u'6509000.0',
             u'now_debt_val': u'0.0', u'capital_out': u'0.0', u'capital_in': u'0.0',
             u'short_return': u'0.0', u'exception_label': u'0',
             u'total_return': u'-8797.670000000113', u'pre_debt_val': u'0.0',
             u'busi_date': u'2017-03-16', u'now_asset_val': u'6500202.33'},
            {u'long_return': u'-115210.41999999993', u'pre_asset_val': u'6458573.36',
             u'now_debt_val': u'0.0', u'capital_out': u'0.0', u'capital_in': u'0.0',
             u'short_return': u'0.0', u'exception_label': u'0',
             u'total_return': u'-115210.41999999993', u'pre_debt_val': u'0.0',
             u'busi_date': u'2017-03-23', u'now_asset_val': u'6343362.94'}])
        a1 = _travel_row2(r, '2017-03-18')['total_return_rate']
        print a1

    def test_checkdata_sql(self):
        # sql = """
        #     select * from adatatest.stock_return_by_month
        # """
        # df = self.spark.sql(sql)
        # df.where(df.trade_id == '66949').
        # select("trade_id", "month", "total_return", "short_return_detail","long_return_detail")
        """
        :return:
        """
        # 比较明细数据和最终结果
        long_return_detail = [{"return": "-4610617.62", "busi_date": "2017-03-30"},
                              {"return": "115204.9", "busi_date": "2017-03-20"},
                              {"return": "13825.62", "busi_date": "2017-03-23"},
                              {"return": "-11643.9", "busi_date": "2017-03-28"},
                              {"return": "9881447.76", "busi_date": "2017-03-31"},
                              {"return": "-78131.67", "busi_date": "2017-03-24"},
                              {"return": "-54748.0", "busi_date": "2017-03-17"},
                              {"return": "-799.0", "busi_date": "2017-03-27"},
                              {"return": "-129825.73", "busi_date": "2017-03-22"},
                              {"return": "-109141.66", "busi_date": "2017-03-16"},
                              {"return": "-10180.0", "busi_date": "2017-03-29"},
                              {"return": "268460.0", "busi_date": "2017-03-21"}]

        short_return_detail = [{"return": "25653969.99", "busi_date": "2017-03-30"},
                               {"return": "5009.0", "busi_date": "2017-03-20"},
                               {"return": "-155650.85", "busi_date": "2017-03-23"},
                               {"return": "-407159.58", "busi_date": "2017-03-28"},
                               {"return": "5575609.0", "busi_date": "2017-03-31"},
                               {"return": "-195419.8", "busi_date": "2017-03-24"},
                               {"return": "4698.96", "busi_date": "2017-03-17"},
                               {"return": "475868.03", "busi_date": "2017-03-27"},
                               {"return": "1261.68", "busi_date": "2017-03-22"},
                               {"return": "-1115.28", "busi_date": "2017-03-16"},
                               {"return": "3139833.07", "busi_date": "2017-03-29"},
                               {"return": "-5811.0", "busi_date": "2017-03-21"}]
        total_return = 39364943.92
        sum_long = 0.0
        sum_short = 0.0
        for long in long_return_detail:
            sum_long += float(long['return'])
        self.assertEquals(int(sum_long), 5273850)

        for short in short_return_detail:
            sum_short += float(short['return'])
        self.assertEquals(int(sum_short), 34091093)

        self.assertTrue(39364943, int(
            sum_long + sum_short))

    def test_sql_data(self):
        sql = """
        select * from (
            select abs(long_return_rate+short_return_rate-total_return_rate) diff_return_rate
            from adata.stock_return_by_month
        ) a
        where diff_return_rate>0.1
        """
        self.assertEquals(self.spark.sql(sql).count(), 0)
