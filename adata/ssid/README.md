# 模块作用

该模块的作用是生成账户诊断相关的量化指标，相关的表为：


* stock_cust_daily_return_ssid
* stock_cust_investment_ability
* stock_cust_investment_rank_score
* stock_cust_diagnosis_sell_time
* stock_cust_diagnosis_sell_ind
* stock_cust_diagnosis_sell_prd
* stock_cust_diagnosis_buy_time
* stock_cust_diagnosis_buy_ind
* stock_cust_diagnosis_buy_prd
* stock_cust_pl_analysis
* stock_cust_trd_quant
* stock_tradebyday_data
* stock_close_tradebyday_by_month


# 运行方式

打包好之后，运行的命令行如下：

```
python auto_run.py <env> <mode> <busi_date>
```

* env表示运行环境，有两种选择分别为“local”：本地运行；“cluster”：集群上运行。
* mode表示运行模式，有两种选择分别为“init”：数据初始化，即根据日期做最初的初始化；“update”：更新给定日期的指标情况。
* busi_date表示运行日期，其格式为“2018-06-10”。