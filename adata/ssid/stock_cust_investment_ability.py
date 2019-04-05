# coding=utf-8
from leek.adata.ssid.ssidjob import LeekSparkJob
from pyspark.sql import functions as F
from leek.common.util import save_data
from pyspark.storagelevel import StorageLevel
from pyspark.sql import Row
import numpy as np


def _travel_row2(row, busi_date=None):
    """
    ps：exception_label是否还需要
    :param row:
    :param busi_date:
    :return:
    """
    dList = row.detail_list
    detailList = sorted(dList, key=lambda x: x['busi_date'])
    sum_total = 0.0
    exception_label = 0
    # 市场上涨 组合收益率
    up_p_return_rate = 1.0
    # 市场上涨 市场收益率
    up_m_return_rate = 1.0
    down_p_return_rate = 1.0
    down_m_return_rate = 1.0
    p_return_rate = 1.0
    m_return_rate = 1.0
    p_return_rate_list = []
    m_return_rate_list = []
    rm_is_always_0 = True
    for item in detailList:
        item_total_return = float(item['total_return'])
        exception_label_item = int(item['exception_label'])
        item_p_return_rate = float(item['total_return_rate'])
        item_m_return_rate = float(item['daily_return_rate'])
        if int(item['exception_label']) > 0:
            exception_label = exception_label | exception_label_item
        #     total_return = 0
        if item_m_return_rate >= 0:
            up_m_return_rate *= (1 + item_m_return_rate)
            up_p_return_rate *= (1 + item_p_return_rate)
        else:
            down_m_return_rate *= (1 + item_m_return_rate)
            down_p_return_rate *= (1 + item_p_return_rate)
        p_return_rate *= (1 + item_p_return_rate)
        m_return_rate *= (1 + item_m_return_rate)
        sum_total += item_total_return
        p_return_rate_list.append(item_p_return_rate)
        m_return_rate_list.append(item_m_return_rate)
        rm_is_always_0 = rm_is_always_0 & (True if item_m_return_rate == 0 else False)
    # 计算市场上涨时的组合收益率
    up_capture = (up_p_return_rate - 1) / (up_m_return_rate - 1) if up_m_return_rate != 1 else 0.0
    down_capture = (down_p_return_rate - 1) / \
                   (down_m_return_rate - 1) if down_m_return_rate != 1 else 0.0
    # 排除计算期内Rm=0的情况。如果计算期内Rm始终为0，则UC=0，DC=0。
    if rm_is_always_0:
        up_capture = 0.0
        down_capture = 0.0
    re_variance = np.var(p_return_rate_list).item()
    return_rate = p_return_rate - 1
    # alpha和beta系数
    rf = 0.03
    Y = np.mat([[prt - rf] for prt in p_return_rate_list])
    betaX = np.mat([[mrt - rf, 1] for mrt in m_return_rate_list])
    alphaBetaRes = np.linalg.lstsq(betaX, Y, rcond=-1)[0]
    beta = alphaBetaRes.item((0, 0)) if len(p_return_rate_list) > 1 else float(1)
    avg_Rp = np.average(p_return_rate_list).item()
    alpha = alphaBetaRes.item((1, 0)) if len(p_return_rate_list) > 1 \
        else p_return_rate_list[0] - m_return_rate_list[0]
    # 夏普比率
    re_std = np.std(p_return_rate_list)
    sharpe_ratio = (np.divide((avg_Rp - rf), re_std)).item() if len(
        p_return_rate_list) > 1 and re_std != 0 else float(1)
    # 来源于大盘的收益/选股收益
    re_from_market = beta * (m_return_rate - 1)
    re_from_selection = return_rate - re_from_market
    # 求解CL
    # 无风险利率
    X = np.mat([(min(mrt - rf, 0), max(mrt - rf, 0), 1) for mrt in m_return_rate_list])
    res = np.linalg.lstsq(X, Y, rcond=-1)[0]
    cl_value = res.item((1, 0)) - res.item((0, 0))
    cl_value = _deal_special_cl_value(cl_value, m_return_rate_list, p_return_rate_list, rf)

    hs300_return_rate = m_return_rate - 1
    model = {"trade_id": row.trade_id, "busi_date": busi_date, "return": sum_total,
             "return_rate": return_rate, "re_variance": re_variance, "up_capture": up_capture,
             "down_capture": down_capture, "alpha": alpha, "beta": beta,
             "sharpe_ratio": sharpe_ratio, "re_from_market": re_from_market,
             "re_from_selection": re_from_selection, "cl_value": cl_value,
             "hs300_return_rate": hs300_return_rate, "exception_label": exception_label}
    return Row(**model)


def _deal_special_cl_value(cl_value, m_return_rate_list, p_return_rate_list, rf):
    if len(p_return_rate_list) == 1:
        cl_value = 1 if m_return_rate_list[0] >= rf else -1
    if len(p_return_rate_list) == 2:
        a = [1 if tem_p >= rf else 0 for tem_p in m_return_rate_list]
        if sum(a) == 0:
            cl_value = -1
        if sum(a) == 1:
            cl_value = 0
        if sum(a) == 2:
            cl_value = 1
    return float(cl_value)


class StockCustInvestmentAbility(LeekSparkJob):

    def __init__(self, spark):
        LeekSparkJob.__init__(self, spark)
        self.stock_cust_investment_ability = self.conf['stock_cust_investment_ability_table']
        self.stock_cust_daily_return = self.conf["stock_cust_daily_return_ssid_table"]
        self.index_price = self.conf["index_price_table"]
        self.step = int(self.confIni.get("task_stock_cust_investment_ability", "travel_part_step"))
        self.table_part = int(self.confIni.get("task_stock_cust_daily_return", "part_numbers"))

    def init_data(self):
        sql = """
            create table if not exists {0}.{1}(
                trade_id	string	comment "客户代码",
                compute_term	bigint	comment "计算周期",
                return	double	comment "收益",
                return_rate	double	comment "收益率",
                re_variance	double	comment "波动率",
                up_capture	double	comment "上行捕获率",
                down_capture	double	comment "下行捕获率",
                alpha	double	comment "alpha系数",
                beta	double	comment "beta系数",
                cl_value	double	comment "CL指标",
                sharpe_ratio	double	comment "夏普比率",
                re_from_market	double	comment "大盘收益",
                re_from_selection	double	comment "选股收益",
                hs300_return_rate	double	comment "沪深300收益率"
            )
            comment '这个表将记录客户每个月的收益情况'
            partitioned by (busi_date   string comment '交易日期')
            STORED AS ORC
        """.format(self.adata, self.stock_cust_daily_return)
        print sql

    def _get_base_data(self, startdate, enddate, part_start, part_end):
        sql = """
                  select  trade_id,collect_list(detail_item) detail_list from (
                        select trade_id,
                            (str_to_map(concat(
                            'busi_date:',a.busi_date,
                            ',exception_label:',exception_label,
                            ',total_return:',total_return,
                            ',total_return_rate:',nvl(total_return_rate,0.0),
                            ',daily_return_rate:',nvl(b.daily_return_rate,0.0)
                            ),",",":")) detail_item
                        from {2}.{3} a left outer join (
                            select * from {2}.{4}
                            where busi_date>='{0}' and busi_date<='{1}' and prd_no='{5}'
                        ) b on a.busi_date=b.busi_date
                        where a.busi_date>='{0}' and a.busi_date<='{1}' and part>={6} and part<={7}
                   ) c
                   group by trade_id
                """
        selectSql = sql.format(startdate, enddate, self.adata, self.stock_cust_daily_return,
                               self.index_price, "000300.SH", part_start, part_end)
        return self.sparkSession.sql(selectSql)

    def daily_compute(self, startdate=None, enddate=None, compute=7):
        step = self.step
        part_start = 0
        part_end = part_start + step - 1
        dropParition = True
        while part_end < self.table_part:
            print 'part start[{}] end[{}],dropParition[{}] table[{}]'.\
                format(part_start, part_end, dropParition, self.stock_cust_investment_ability)
            self._cal(startdate, enddate, compute, part_start, part_end, dropParition)
            part_start = part_end + 1
            part_end = part_start + step - 1
            dropParition = False

    def _cal(self, startdate, enddate, compute, part_start, part_end, dropParition):
        """
        daily_compute
        :param startdate:
        :param enddate:
        :return:
        """
        # calc 2 return，return_rate
        dfBase = self._get_base_data(startdate, enddate, part_start, part_end)
        # 计算long_return，short_return，total_return
        dfBaseRdd = dfBase.rdd.map(lambda rows: _travel_row2(rows, enddate))
        if dfBaseRdd is None or dfBaseRdd.isEmpty():
            print "dfBaseRdd empty,after _travel_row2 "
            return
        self._cal_final_result(dfBaseRdd, compute, enddate, dropParition)

    def _cal_final_result(self, dfBaseRdd, compute, enddate, dropParition):
        prefinalDf = self.sparkSession.createDataFrame(dfBaseRdd).withColumn("compute_term",
                                                                             F.lit(compute))
        prefinalDf.persist(StorageLevel.DISK_ONLY).count()
        dropPartitionSql = "alter table {0}.{1} drop " \
                           "if exists  partition(busi_date= '{2}',compute_term='{3}')"\
            .format(self.adata, self.stock_cust_investment_ability, enddate, compute)
        save_data(self.sparkSession, self.adata, self.stock_cust_investment_ability, enddate,
                  prefinalDf, partitonByName=["busi_date", "compute_term"],
                  dropPartitonSql=dropPartitionSql, defaultDropPartition=dropParition)
