# coding=utf-8
from leek.adata.sbs.sbsjob import LeekSparkJob
from pyspark.sql import Row, functions as sqlf
from leek.common.util import save_data, full_outer_join, get_date
from pyspark.storagelevel import StorageLevel
from leek.fdata.ctdr.util import sort_unclose_data, sort_close_data, \
    sort_unclose_cal, compute_short_trade, amortize_long_trade, \
    amortize_short_trade, compute_long_trade, trd_add_trans, filter_data, \
    flat_data
from leek.common.util import days_between


def _travel_long_iter(rows, busi_date, diff_days):
    for item in rows:
        yield _travel_long_iter_row(item.asDict(True), busi_date, diff_days)


def _travel_short_iter(rows, busi_date, diff_days):
    for item in rows:
        yield _travel_short_iter_row(item.asDict(True), busi_date, diff_days)


def _travel_short_iter_row(data, busi_date, weighted_term_diff):
    open_detail = data["open_detail"] if data["open_detail"] is not None else []
    trd_detail_list = sorted(data["trd_detail_list"],
                             key=lambda x: x["timestamp"]) \
        if data["trd_detail_list"] is not None else []
    trd_ind = 0
    open_ind = 0
    tol_bias = 1
    liab_qty = data["liab_qty"] if data["liab_qty"] is not None else 0
    mkt_val = data["mkt_val"] if data["mkt_val"] is not None else 0.0
    for pos in open_detail:
        # 这里先计算是因为后面的unclose_qty会变
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = (weighted_term_diff - 1) * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)
    while trd_ind < len(trd_detail_list):
        trd = trd_detail_list[trd_ind]
        trd_qty = int(trd['trd_qty'])
        amortize_label = int(trd['amortize_label'])
        if amortize_label == 1:
            amortize_short_trade(open_detail, open_ind, trd)
            trd_ind += 1
        elif trd_qty < 0:
            # 新增买入都要放入到待计算的数据中
            open_ind = trd_add_trans(open_detail, trd, busi_date, liab_qty, open_ind)
            trd_ind += 1
        elif trd_qty > 0:
            if open_ind < len(open_detail):
                pos = open_detail[open_ind]
                pos, trd, open_ind, trd_ind = compute_short_trade(pos, trd,
                                                                  open_ind,
                                                                  trd_ind)
            else:
                trd_ind += 1
    for pos in reversed(open_detail[open_ind:]):
        if int(pos["unclose_qty"]) >= liab_qty - tol_bias:
            if abs(int(pos["unclose_qty"]) - liab_qty) <= tol_bias:
                current_qty = liab_qty
            else:
                current_qty = int(pos["unclose_qty"])
            pos["unclose_qty"] = str(current_qty)
            if liab_qty != 0:
                pos["unclose_amt"] = str(
                    float(current_qty) / liab_qty * mkt_val)
                mkt_val *= (liab_qty - float(current_qty)) / liab_qty
                liab_qty -= current_qty
            else:
                pos["unclose_amt"] = str(0.0)
                mkt_val = 0.0
                liab_qty = 0
        else:
            pos["exception_label"] = str(1)
            # 异常处理一下
            ratio = (int(pos["unclose_qty"]) - liab_qty) / float(
                pos["unclose_qty"])
            pos["close_amt"] = str(
                float(pos["close_amt"]) - float(pos["unclose_amt"]) * ratio)
            pos["unclose_qty"] = str(liab_qty)
            pos["unclose_amt"] = str(mkt_val)
            liab_qty = 0
            mkt_val = 0.0
    if liab_qty < -1 * tol_bias:
        open_detail.append(
            {"open_date": busi_date, "open_type": "trade_exception",
             "orig_trd_qty": str(liab_qty), "orig_trd_amt": str(mkt_val),
             "trd_qty": str(liab_qty), "trd_amt": str(mkt_val),
             "close_amt": str(0.0), "unclose_qty": str(liab_qty),
             "unclose_amt": str(mkt_val), "weighted_term": str(0.0),
             "exception_label": str(1)})
    for pos in open_detail:
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = 1 * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)
    model = {"trade_id": data["trade_id"], "secu_acc_id": data["secu_acc_id"],
             "prd_no": data["prd_no"], "busi_date": busi_date,
             "open_detail": open_detail}
    return model


def _travel_long_iter_row(data, busi_date, weighted_term_diff):
    # 在open_detail里面还有元素的情况下，对trd_detail里面的内容遍历，假设当前原始为x，
    # 如果x是买入（trd_qty为正）而且amortize_label=0，
    # 则将其增加到open_detail里。这里需要注意的是，
    # 如果open_detail里最后一个元素y的日期就是T-1日，则需要将x和y进行加总处理
    open_detail = data["open_detail"] if data["open_detail"] is not None else []
    trd_detail_list = sorted(data["trd_detail_list"],
                             key=lambda x: x["timestamp"]) \
        if data["trd_detail_list"] is not None else []
    trd_ind = 0
    open_ind = 0
    qty = data["qty"] if data["qty"] is not None else 0
    mkt_val = data["mkt_val"] if data["mkt_val"] is not None else 0.0
    for pos in open_detail:
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = (weighted_term_diff - 1) * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)
    while trd_ind < len(trd_detail_list):
        trd = trd_detail_list[trd_ind]
        trd_qty = int(trd['trd_qty'])
        amortize_label = int(trd['amortize_label'])
        if amortize_label == 1:
            amortize_long_trade(open_detail, open_ind, trd)
            trd_ind += 1
        elif trd_qty >= 0:
            # 新增买入都要放入到待计算的数据中
            open_ind = trd_add_trans(open_detail, trd, busi_date, qty, open_ind)
            trd_ind += 1
        elif trd_qty < 0:
            if open_ind < len(open_detail):
                pos = open_detail[open_ind]
                pos, trd, open_ind, trd_ind = compute_long_trade(pos, trd,
                                                                 open_ind,
                                                                 trd_ind)
            else:
                trd_ind += 1
    tol_bias = 1
    for pos in reversed(open_detail[open_ind:]):
        # 这里加1是为了处理进位问题
        if int(pos["unclose_qty"]) <= qty + tol_bias:
            if abs(int(pos["unclose_qty"]) - qty) <= tol_bias:
                current_qty = qty
            else:
                current_qty = int(pos["unclose_qty"])
            pos["unclose_qty"] = str(current_qty)
            if qty != 0:
                pos["unclose_amt"] = str(float(current_qty) / qty * mkt_val)
                mkt_val *= (qty - float(current_qty)) / qty
                qty -= current_qty
            else:
                pos["unclose_amt"] = str(0.0)
                mkt_val = 0.0
                qty = 0
        else:
            pos["exception_label"] = str(1)
            # 异常处理一下
            ratio = (int(pos["unclose_qty"]) - qty) / float(pos["unclose_qty"])
            pos["close_amt"] = str(
                float(pos["close_amt"]) + float(pos["unclose_amt"]) * ratio)
            pos["unclose_qty"] = str(qty)
            pos["unclose_amt"] = str(mkt_val)
            qty = 0
            mkt_val = 0.0
    if qty > tol_bias:
        open_detail.append(
            {"open_date": busi_date, "open_type": "trade_exception",
             "orig_trd_qty": str(qty), "orig_trd_amt": str(mkt_val),
             "trd_qty": str(qty), "trd_amt": str(mkt_val),
             "close_amt": str(0.0), "unclose_qty": str(qty),
             "unclose_amt": str(mkt_val), "weighted_term": str(0.0),
             "exception_label": str(1)})
    for pos in open_detail:
        ratio = float(pos["unclose_qty"]) / float(pos["trd_qty"])
        _add_days = 1 * ratio
        pos["weighted_term"] = str(float(pos["weighted_term"]) + _add_days)
    model = {"trade_id": data["trade_id"], "secu_acc_id": data["secu_acc_id"],
             "prd_no": data["prd_no"], "busi_date": busi_date,
             "open_detail": open_detail}
    return model


def trans_short_close(data):
    for item in data["open_detail"]:
        a = flat_data(data, item)
        a["close_date"] = data["busi_date"]
        if a["trd_amt"] != 0 :
            a["return"] = -a["trd_amt"] - a["close_amt"]
            a["return_rate"] = a["return"] / a["close_amt"]
        else:
            a["return"] = 0.0
            a["return_rate"] = 0.0
        yield Row(**a)


def trans_long_close(data):
    for item in data["open_detail"]:
        a = flat_data(data, item)
        a["close_date"] = data["busi_date"]
        if a["trd_amt"] != 0:
            a["return"] = a["close_amt"] - a["trd_amt"]
            a["return_rate"] = a["return"] / a["trd_amt"]
        else:
            a["return"] = 0.0
            a["return_rate"] = 0.0
        yield Row(**a)


def trans_short_unclose(data):
    """
    """
    for item in data["open_detail"]:
        a = flat_data(data, item)
        a["unclose_qty"] = int(item["unclose_qty"])
        a["unclose_amt"] = float(item["unclose_amt"])
        a["close_qty"] = a["trd_qty"] - a["unclose_qty"]
        if a["close_amt"] - a["unclose_amt"] !=0:
            a["return"] = -a["trd_amt"] - (a["close_amt"] - a["unclose_amt"])
            a["return_rate"] = a["return"] / (a["close_amt"] - a["unclose_amt"])
        else:
            a["return"] = 0.0
            a["return_rate"] = 0.0
        yield Row(**a)


def _init_trans(rowIter, type):
    for data in rowIter:
        a = dict()
        qty = data["qty"] if type == 'long' else data['liab_qty']
        a["trade_id"] = data["trade_id"]
        a["secu_acc_id"] = data["secu_acc_id"]
        a["prd_no"] = data["prd_no"]
        a["busi_date"] = data["busi_date"]
        a["open_detail"] = [
            {"open_date": data["busi_date"], "open_type": "init",
             "orig_trd_qty": str(qty), "orig_trd_amt": str(data["mkt_val"]),
             "trd_qty": str(qty), "trd_amt": str(data["mkt_val"]),
             "close_amt": str(0.0), "unclose_qty": str(qty),
             "unclose_amt": str(data["mkt_val"]), "weighted_term": str(1.0),
             "exception_label": str(0)}]
        yield Row(**a)


def trans_long_unclose(data):
    """
    """
    for item in data["open_detail"]:
        a = flat_data(data, item)
        a["unclose_qty"] = int(item["unclose_qty"])
        a["unclose_amt"] = float(item["unclose_amt"])
        a["close_qty"] = a["trd_qty"] - a["unclose_qty"]
        if a["trd_amt"] != 0:
            a["return"] = a["close_amt"] + a["unclose_amt"] - a["trd_amt"]
            a["return_rate"] = a["return"] / a["trd_amt"]
        else:
            a["return"] = 0.
            a["return_rate"] = 0.
        yield Row(**a)


class StockCloseTradeByDay(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_close_tradebyday_long_data = self.conf[
            "stock_close_tradebyday_long_data_table"]
        self.stock_close_tradebyday_short_data = self.conf[
            "stock_close_tradebyday_short_data_table"]
        self.stock_unclose_tradebyday_long_data = self.conf[
            "stock_unclose_tradebyday_long_data_table"]
        self.stock_unclose_tradebyday_short_data = self.conf[
            "stock_unclose_tradebyday_short_data_table"]
        self.stock_unclose_tradebyday_long_cal = self.conf[
            "stock_unclose_tradebyday_long_cal_table"]
        self.stock_unclose_tradebyday_short_cal = self.conf[
            "stock_unclose_tradebyday_short_cal_table"]
        self.cash_flow_detail = self.conf['cash_flow_table']
        self.asset_table = self.conf['asset_table']
        self.debt_table = self.conf["debt_table"]

    def init_data(self, busi_date=None):
        # 在最初启动的时候，需要将ODATA.stock_asset_holding里T-2
        # 的数据转换为FDATA.stock_unclose_tradebyday_long_cal里T-2的数据
        sqlCmd = """
              select
                trade_id,secu_acc_id,prd_no,busi_date,
                sum({3}) {3},sum(mkt_val) mkt_val
              from {0}.{1} where busi_date='{2}' and prd_no!='0.0'
              group by trade_id,secu_acc_id,prd_no,busi_date
            """
        assetDf = self.sparkSession.sql(
            sqlCmd.format(self.odata, self.asset_table, busi_date, "qty"))
        assetRdd = assetDf.rdd.mapPartitions(lambda x: _init_trans(x, "long"))
        # self.sparkSession.createDataFrame(assetRdd).show()
        if assetRdd.count() >0:
            self.save_all_data(assetRdd, busi_date, "long")
        debtDf = self.sparkSession.sql(
            sqlCmd.format(self.odata, self.debt_table, busi_date, "liab_qty"))
        debtRdd = debtDf.rdd.mapPartitions(lambda x: _init_trans(x, "short"))
        if debtRdd.count() >0:
            self.save_all_data(debtRdd, busi_date, "short")

    def daily_compute(self, startdate=None, enddate=None):
        """
        daily_compute
        :param startdate:
        :param enddate:
        :return:
        """
        # 月度计算，需要从一号开始
        yesterday = get_date(self.date_order, self.order_date, enddate, -1)
        diff_days = days_between(enddate, yesterday)
        LongBaseDf = self._get_base_data(self.sparkSession,
                                         self.stock_unclose_tradebyday_long_cal,
                                         enddate, self.asset_table, "long")
        longBaseRdd = LongBaseDf.rdd.mapPartitions(
            lambda rows: _travel_long_iter(rows, enddate, diff_days))
        longBaseRdd.persist(StorageLevel.DISK_ONLY)
        if longBaseRdd.count() >0 :
            self.save_all_data(longBaseRdd, enddate, "long")
        ShortBaseDf = \
            self._get_base_data(self.sparkSession,
                                self.stock_unclose_tradebyday_short_cal,
                                enddate, self.debt_table, "short")
        shortBaseRdd = ShortBaseDf.rdd.mapPartitions(
            lambda rows: _travel_short_iter(rows, enddate, diff_days))
        shortBaseRdd.persist(StorageLevel.DISK_ONLY)
        if shortBaseRdd.count() > 0:
            self.save_all_data(shortBaseRdd, enddate, "short")

    def _get_base_data(self, spark, table, enddate, odataTable, type):
        sqlTemp = "select * from {0}.{1} where busi_date='{2}'"
        startdate = get_date(self.date_order, self.order_date, enddate, -1)
        unCloseCalDf = spark.sql(sqlTemp.format(self.fdata, table, startdate))
        trd_type = "long_related" if type == 'long' else "short_related"
        cashDetailDf = spark.sql(
            sqlTemp.format(self.odata, self.cash_flow_detail, enddate)).filter(
            "trd_type='%s' and prd_no!='0.0'" % trd_type).filter(
            "trd_qty!=0 or amortize_label!=0 ").persist(
            storageLevel=StorageLevel.DISK_ONLY).\
            withColumn("trd_detail",
                       sqlf.struct("timestamp", "trd_qty", "trd_cash_flow",
                                   "amortize_label"))\
            .groupBy("trade_id", "secu_acc_id", "prd_no").agg(
            sqlf.collect_list("trd_detail").alias("trd_detail_list"))
        cashDetailDf.persist(StorageLevel.DISK_ONLY).count()
        moneyDf = spark.sql(
            sqlTemp.format(self.odata, odataTable, enddate)).filter(
            "prd_no!='0.0'").select("trade_id", "secu_acc_id", "prd_no",
                                    "qty" if type == 'long' else "liab_qty",
                                    "mkt_val")
        tempDf = full_outer_join(unCloseCalDf, cashDetailDf,
                                 ["trade_id", "secu_acc_id", "prd_no"])
        baseDf = full_outer_join(tempDf, moneyDf,
                                 ["trade_id", "secu_acc_id", "prd_no"])
        baseDf.persist(StorageLevel.DISK_ONLY).count()
        return baseDf

    def save_all_data(self, dataRdd, busi_date, type):
        if type == 'long':
            filterFunc = lambda t: int(t["unclose_qty"]) > 0
            close_table = self.stock_close_tradebyday_long_data
            unclose_table = self.stock_unclose_tradebyday_long_data
            unclose_cal_table = self.stock_unclose_tradebyday_long_cal
            flatCloseFunc = trans_long_close
            flatUnCloseFunc = trans_long_unclose
        else:
            filterFunc = lambda t: int(t["unclose_qty"]) < 0
            close_table = self.stock_close_tradebyday_short_data
            unclose_table = self.stock_unclose_tradebyday_short_data
            unclose_cal_table = self.stock_unclose_tradebyday_short_cal
            flatCloseFunc = trans_short_close
            flatUnCloseFunc = trans_short_unclose
        self.save_unclose_cal(dataRdd, busi_date, filterFunc, sort_unclose_cal,
                              unclose_cal_table)
        self.save_unclose_data(dataRdd, busi_date, filterFunc,
                               sort_unclose_data, flatUnCloseFunc,
                               unclose_table)
        self.save_close_data(dataRdd, busi_date,
                             lambda t: int(t["unclose_qty"]) == 0,
                             sort_close_data, flatCloseFunc, close_table)

    def save_close_data(self, data, busi_date, filter_func, sort_func,
                        trans_close_func, close_table):
        """
        """
        data = data.map(lambda x: filter_data(x, filter_func))
        data = data.filter(lambda x: len(x["open_detail"]) > 0)
        if data.count() > 0:
            data = data.flatMap(trans_close_func).toDF()
            data = sort_func(data)
            save_data(self.sparkSession, self.fdata, close_table, busi_date,
                      data)
        else:
            pass

    def save_unclose_data(self, data, busi_date, filter_func, sort_func,
                          _trans_unclose_func, unclose_table):
        """
        """
        data = data.map(lambda x: filter_data(x, filter_func))
        data = data.filter(lambda x: len(x["open_detail"]) > 0)
        if data.count() > 0:
            data = data.flatMap(_trans_unclose_func).toDF()
            data = sort_func(data)
            save_data(self.sparkSession, self.fdata, unclose_table, busi_date,
                      data)
        else:
            pass

    def save_unclose_cal(self, data, busi_date, filter_func, sort_func,
                         unclose_cal_table):
        """
        """
        data = data.map(lambda x: filter_data(x, filter_func))
        data = data.filter(lambda x: len(x["open_detail"]) > 0)
        data = data.map(lambda x: Row(**x))
        if data.count() > 0:
            data = data.toDF()
            data = sort_func(data)
            save_data(self.sparkSession, self.fdata, unclose_cal_table,
                      busi_date, data)
