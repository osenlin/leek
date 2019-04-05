create table if not exists {0}.{1} (
    trade_id  string,
    busi_date  string,
    prd_no  string,
    prd_ind  string,
    trd_type  bigint,
    pre_mkt_val  double,
    now_mkt_val  double,
    pos_cash_flow  double,
    neg_cash_flow  double,
    capital_in  double,
    capital_out  double,
    exception_label  bigint,
    return  double,
    return_rate  double
)
comment '记录客户每日在股票市场上的持仓和每支股票的收益情况'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC
