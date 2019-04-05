from testbase import testBase


class GenerateError2DailyCheckException(testBase):

    def test_stat_generate_error(self):
        sqltmp = """
            select trd_type,busi_date,count(1) error_count
            from odata.stock_generate_error
            where busi_date>='2017-03-16' and busi_date<='2017-04-14'
            group by trd_type,busi_date
            order by busi_date
        """

        df = self.sparkSession.sql(sqltmp)
        if testBase.logLevel != 'debug':
            df.write.format("orc").mode("overwrite").saveAsTable("test.stock_generate_error_stat_1")

    def test_stat(self):
        sql = """
            select *
            from fdata.stock_daily_check_data_rg
            where qty_exception=0 and return_rate_exception!=0
        """
        self.sparkSession.sql(sql)

    def test_stat_prd_0_0(self):

        sql = """
            select * from fdata.stock_daily_check_data_rg
            where prd_no!='0.0'
        """
        self.sparkSession.sql(sql)

    def test_checkExceptionTypeByTrdType(self):

        sqlTmp = """
            select * from (
                select a.trd_type,a.busi_date,a.error_count,b.data_count,
                    a.error_count-b.data_count diff_count from (
                    select trd_type,busi_date,count(1) error_count
                    from odata.stock_generate_error
                    where busi_date='{0}'
                    group by trd_type,busi_date) a
                full outer join (
                    select trd_type,busi_date,count(1) data_count
                    from fdata.stock_daily_check_exception_rg
                    where busi_date='{0}'
                    group by trd_type,busi_date
                ) b on a.busi_date=b.busi_date and a.trd_type=b.trd_type
            ) a where diff_count!=0
            """.format(self.caclData)

        df = self.sparkSession.sql(sqlTmp)
        if testBase.logLevel != 'debug':
            for row in df.collect():
                self.assertEqual(
                    int(row['diff_count']), 0,
                    msg="trd_type[{0}],stock_generate_error[{1}],stock_daily_check_exception[{2}]".
                        format(row['trd_type'], row['error_count'], row['data_count']))

    def test_checkQty(self):
        sqlTmp = """
            select a.trade_id,a.secu_acc_id,a.prd_no,a.trd_type,a.busi_date,
            error_qty,pre_qty,trd_qty,now_qty,
                   pre_qty+trd_qty+error_qty-now_qty diff_count
            from (
                select trade_id,secu_acc_id,prd_no,trd_type,busi_date,
                  sum(nvl(error_qty,0)) error_qty
                from odata.stock_generate_error
                where busi_date='{0}'
                group by trade_id,secu_acc_id,prd_no,trd_type,busi_date) a
            inner join (
                select trade_id,secu_acc_id,prd_no,trd_type,busi_date,
                    sum(nvl(pre_qty,0)) pre_qty,
                    sum(nvl(trd_qty,0)) trd_qty,
                    sum(nvl(now_qty,0)) now_qty
                from fdata.stock_daily_check_exception_rg
                where busi_date='{0}'
                group by trade_id,secu_acc_id,prd_no,trd_type,busi_date
            ) b
            on a.busi_date=b.busi_date
            and  a.trade_id=b.trade_id
            and a.secu_acc_id=b.secu_acc_id
            and a.prd_no=b.prd_no
            and a.trd_type=b.trd_type
        """.format(self.caclData)
        df = self.sparkSession.sql(sqlTmp)
        if testBase.logLevel != 'debug':
            for row in df.collect():
                self.assertEqual(
                    int(row['diff_count']), 0,
                    "trade_id[{0}],secu_acc_id[{1}],prd_no[{2}],diff_count[{3}]"
                    .format(row['trade_id'], row['secu_acc_id'], row['prd_no'],
                            row['diff_count']))

    def test_checkReturnRateNum(self):
        sqlTmp = """
          select * from (
            select trade_id,prd_no,count(1) num from test.stock_daily_check_data_324_5
            where return_rate_exception=1 and busi_date='{0}'
            group by trade_id,prd_no
          ) a order by num DESC
          limit 10
        """.format(self.caclData)
        self.sparkSession.sql(sqlTmp)

    def test_checkReturnRateMM(self):
        sqlTmp = """
          select * from (
            select trade_id,prd_no,max(return_rate) mx,min(return_rate) mi
            from test.stock_daily_check_data_324_5
            where return_rate_exception=1 and busi_date='{0}'
            group by trade_id,prd_no
          ) a
        """.format(self.caclData)
        self.sparkSession.sql(sqlTmp)

    def test_checkReturnRateMMALL(self):
        sqlTmp = """
          select * from (
            select max(return_rate) mx,min(return_rate) mi from test.stock_daily_check_data_324_5
            where return_rate_exception=1 and busi_date='{0}'
          ) a
        """.format(self.caclData)
        self.sparkSession.sql(sqlTmp)

    def test_checkReturnRateMMALL_detail(self):
        sqlTmp = """
            select * from test.stock_daily_check_data_cap
            where return_rate>10 and return_rate_exception==0
        """
        self.sparkSession.sql(sqlTmp)

        sqlTmp = """
            select *from test.stock_daily_check_data_324_5
            where return_rate>10 and return_rate_exception=1
        """
        self.sparkSession.sql(sqlTmp)

        sqlTmp1 = """
        select * from odata.stock_debt_holding where busi_date<='{0}'
        and trade_id=34203 and prd_no='2.002783'
        """.format(self.caclData)
        self.sparkSession.sql(sqlTmp1)

        sqlTmp2 = """
        select * from odata.stock_cash_flow_detail where  busi_date<='{0}'
              and trade_id=34203 and prd_no='2.002783'
        """.format(self.caclData)
        self.sparkSession.sql(sqlTmp2)

        sqlTmp3 = """
                select * from test.stock_cash_flow_324_5 where busi_date<='{0}'
                    and  trade_id=34203 and prd_no='2.002783'
                """.format(self.caclData)
        self.sparkSession.sql(sqlTmp3)

        sqlTmp4 = """
        select * from (
         select trd_type,busi_date,count(1) error_count
            from odata.stock_generate_error
            where busi_date>='2017-03-16' and busi_date<='2017-04-14'
            group by trd_type,busi_date
          ) a
         order by  busi_date
        """
        self.sparkSession.sql(sqlTmp4)
