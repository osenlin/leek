select * from clean_tool_jz.stock_cust_daily_return a
full outer join jinzheng_w_adata.stock_cust_daily_return b
on a.trade_id=b.trade_id and a.busi_date=b.busi_date
where abs(a.total_return-b.total_return)>0.01 or abs(a.total_return_rate-b.total_return_rate)>0.01

select * from clean_tool_jz.stock_cash_flow a
full outer join jinzheng_w_fdata.stock_cash_flow b
on a.trade_id=b.trade_id and a.busi_date=b.busi_date and a.secu_acc_id=b.secu_acc_id
and a.prd_no=b.prd_no and a.trd_type=b.trd_type
where a.trd_cash_flow!=b.trd_cash_flow and a.int_tax_in!=b.int_tax_in
and a.int_tax_out!=b.int_tax_out;

spark.sql("""
select a.trade_id,b.trade_id,a.return,b.return,a.return_rate,b.return_rate
from clean_tool_jz.stock_cust_daily_holding a
inner  join jinzheng_w_adata.stock_cust_daily_holding b
on a.trade_id=b.trade_id and a.busi_date=b.busi_date and a.trd_type=b.trd_type and a.prd_no=b.prd_no
where (abs(a.return-b.return)>0.01 or abs(a.return_rate-b.return_rate)>0.01)
and a.busi_date>='2017-06-01' and a.busi_date<='2017-08-01'
""").show()


spark.sql("""
select a.trade_id,a.secu_acc_id,a.prd_no,a.return,b.return,a.return_rate,b.return_rate from clean_tool_jz.stock_daily_check_data a
inner join jinzheng_w_fdata.stock_daily_check_data b
on a.trade_id=b.trade_id and a.busi_date=b.busi_date and a.secu_acc_id=b.secu_acc_id
and a.prd_no=b.prd_no and a.trd_type=b.trd_type
where (abs(a.return-b.return)>0.01 or abs(a.return_rate-b.return_rate)>0.0001)
and a.busi_date>='2017-06-01' and a.busi_date<='2017-12-01'
""").show()

spark.sql("""
select a.trade_id,a.exception_label,b.exception_label,a.return_type_1,b.return_type_1,a.return_type_2,b.return_type_2,
a.return_rate_type_1,b.return_rate_type_1,a.return_rate_type_2,b.return_rate_type_2
from clean_tool_jz.asset_cust_daily_return a
inner join jinzheng_w_adata.asset_cust_daily_return b
on a.trade_id=b.trade_id and a.busi_date=b.busi_date and a.exception_label=b.exception_label
where (abs(a.return_type_1-b.return_type_1)>0.01
or abs(a.return_type_2-b.return_type_2)>0.01
or abs(a.return_rate_type_1-b.return_rate_type_1)>0.01
or abs(a.return_rate_type_2-b.return_rate_type_2)>0.01)
 and a.busi_date>='2017-06-01' and a.busi_date<='2017-12-15'
""").show()

spark.sql("""
select count(1) from jinzheng_w_adata.asset_cust_daily_return a
where a.busi_date>='2017-06-01' and a.busi_date<='2017-12-15'
""").show()

spark.sql("""
select count(1) from clean_tool_jz.asset_cust_daily_return a
where a.busi_date>='2017-06-01' and a.busi_date<='2017-12-15'
""").show()


spark.sql("""
select count(1) from jinzheng_w_fdata.stock_daily_check_data a
where a.busi_date>='2017-06-01' and a.busi_date<='2017-12-15'
""").show()

spark.sql("""
select count(1) from clean_tool_jz.stock_daily_check_data a
where a.busi_date>='2017-06-01' and a.busi_date<='2017-12-15'
""").show()

spark.sql("""
select count(1) from jinzheng_w_adata.asset_cust_daily_return_bak a
where a.busi_date>='2017-06-01' and a.busi_date<='2017-12-15'
""").show()

spark.sql("""
select count(1) from clean_tool_jz.asset_cust_daily_return_bak a
where a.busi_date>='2017-06-01' and a.busi_date<='2017-12-15'
""").show()



spark.sql("""
select a.trade_id,a.exception_label,b.exception_label,a.return_type_1,b.return_type_1,a.return_type_2,b.return_type_2,
a.return_rate_type_1,b.return_rate_type_1,a.return_rate_type_2,b.return_rate_type_2
from clean_tool_jz.asset_cust_daily_return_bak a
inner join jinzheng_w_adata.asset_cust_daily_return_bak b
on a.trade_id=b.trade_id and a.busi_date=b.busi_date and a.exception_label=b.exception_label
where (abs(a.return_type_1-b.return_type_1)>0.01
or abs(a.return_type_2-b.return_type_2)>0.01
or abs(a.return_rate_type_1-b.return_rate_type_1)>0.01
or abs(a.return_rate_type_2-b.return_rate_type_2)>0.01)
 and a.busi_date>='2017-06-01' and a.busi_date<='2017-12-15'
""").show()


