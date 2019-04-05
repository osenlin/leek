create table if not exists test.stock_cash_flow(
trade_id string comment '客户代码',
secu_acc_id   string comment '股票账户',
prd_no   string comment '资产代码',
busi_flag_code   string comment '业务代码',
busi_flag_name   string comment '业务名称',
trd_qty   bigint comment '交易数量',
pos_trd_qty   bigint comment '正交易数量',
neg_trd_qty   bigint comment '负交易数量',
trd_cash_flow   double comment '交易金额',
pos_cash_flow   double comment '正交易金额',
neg_cash_flow   double comment '负交易金额',
trd_fee   double comment '交易费用',
cash_flow_modi_label   double comment '现金流是否修正',
trd_type   string comment '交易类型'
)
comment '现金流合并模块'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC;

create table if not exists test.stock_daily_check_data(
trade_id	string comment '客户代码',
secu_acc_id	string comment '股票账户',
prd_no	string comment '资产代码',
pre_qty	bigint comment '前一交易日持仓（或负债）数量',
trd_qty	bigint comment '交易数量',
now_qty	bigint comment '当前持仓（或负债）数量',
pre_mkt_val	double comment '前一交易日持仓（或负债）市值',
now_mkt_val	double comment '当前持仓（或负债）市值',
trd_cash_flow	double comment '交易金额',
pos_cash_flow	double comment '正交易金额',
neg_cash_flow	double comment '负交易金额',
busi_flag_code	string comment '业务代码',
return	double comment '收益',
return_rate	double comment '收益率',
qty_exception	bigint comment '持仓量核对异常',
return_rate_exception	bigint comment '收益率核对异常',
trd_type	string comment '交易类型'
)
comment '账目核对模块'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC;


create table if not exists test.stock_daily_check_report(
exception_pv	bigint	comment '出现异常的资产数',
exception_uv	bigint	comment '出现异常的客户数',
exception_pv_rate	double	comment '异常资产比例',
exception_uv_rate	double	comment '异常客户比例',
max_return	double	comment '最大异常收益',
min_return	double	comment '最小异常收益',
max_return_rate	double	comment '最大异常收益率',
min_return_rate	double	comment '最小异常收益率',
exception_type	string	comment '异常类型'
)
comment '账目核对统计与预警模块设计'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC;


create table if not exists test.stock_daily_check_exception(
trade_id	string comment '客户代码',
secu_acc_id	string comment '股票账户',
prd_no	string comment '资产代码',
pre_qty	bigint comment '前一交易日持仓（或负债）数量',
trd_qty	bigint comment '交易数量',
now_qty	bigint comment '当前持仓（或负债）数量',
pre_mkt_val	double comment '前一交易日持仓（或负债）市值',
now_mkt_val	double comment '当前持仓（或负债）市值',
trd_cash_flow	double comment '交易金额',
pos_cash_flow	double comment '正交易金额',
busi_flag_code	string comment '业务代码',
exception_type	string comment '异常类型',
trd_type	string comment '交易类型'
)
comment '持仓核对和收益率核对异常明细数据'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC;



create table if not exists test.stock_close_prd_long_data(
	trade_id	string   comment '客户代码',
	secu_acc_id	string  comment '股票账户',
	prd_no	string  comment '资产代码',
	close_date	string  comment '清仓日期',
	open_date	string  comment '建仓日期',
	holding_term	bigint  comment '持股时长',
	return	double  comment '收益',
	return_rate	double  comment '收益率',
	total_in	double  comment '累计投入',
	total_out	double  comment '累计收入',
	trd_detail	array<map<string,string>>  comment '交易明细',
	exception_label	bigint  comment '异常标签'
)
comment '每日清仓股票表'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC;


create table if not exists test.stock_unclose_prd_long_data(
	trade_id	string  comment '客户代码',
	secu_acc_id	string  comment '股票账户',
	prd_no	string  comment '资产代码',
	open_date	string  comment '建仓日期',
	holding_term	bigint  comment '持仓时长',
	return	double  comment '收益',
	return_rate	double  comment '收益率',
	total_in	double  comment '累计投入',
	total_out	double  comment '累计收入',
	remain_qty	bigint  comment '剩余股数',
	remain_val	double  comment '剩余市值',
	trd_detail	array<map<string,string>>  comment '交易明细',
	exception_label	bigint  comment '异常标签'
)
comment '每日未清仓股票表'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC;

create table if not exists test.stock_close_trade_long_data(
	trade_id	string     comment '客户代码',
	secu_acc_id	string  comment '股票账户',
	prd_no	string  comment '资产代码',
	open_date	string  comment '交易开始日期',
	orig_trd_qty	bigint  comment '原始开仓 数量',
	orig_trd_amt	double  comment '原始开仓金额',
	trd_qty	bigint  comment '修正开仓数量',
	trd_amt	double  comment '修正开仓金额',
	close_amt	double  comment '闭仓金额',
	return	double  comment '收益',
	return_rate	double  comment '收益率',
	weighted_term	double  comment '加权持仓时长',
	exception_label	bigint  comment '异常标签'
)
comment '每日完结交易表-记录每日完结的交易收益明细'
partitioned by (close_date   string comment '交易完结日期')
STORED AS ORC;


create table if not exists test.stock_unclose_trade_long_data(
	trade_id	string  comment '客户代码',
	secu_acc_id	string  comment '股票账户',
	prd_no	string  comment '资产代码',
	open_date	string  comment '交易开始日期',
	orig_trd_qty	bigint  comment '原始开仓 数量',
	orig_trd_amt	double  comment '原始开仓金额',
	trd_qty	bigint  comment '修正开仓数量',
	trd_amt	double	  comment '修正开仓金额',
	close_qty	bigint	  comment '已闭仓的数量',
	close_amt	double  comment '已闭仓的金额',
	unclose_qty	bigint  comment '余下未闭仓的数据',
	unclose_amt	double  comment '余下未闭仓的金额',
	return	double  comment '收益',
	return_rate	double  comment '收益率',
	weighted_term	double  comment '加权持仓时长',
	exception_label	bigint  comment '异常标签'
)
comment '每日未完结交易表'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC;



create table if not exists test.stock_unclose_trade_long_cal(
	trade_id	string comment '客户代码',
	secu_acc_id	string comment '股票账户',
	prd_no	string comment '资产代码',
	open_detail	string comment '交易细节'
)
comment '每日未完结交易计算表'
partitioned by (busi_date   string comment '交易日期')
STORED AS ORC;

