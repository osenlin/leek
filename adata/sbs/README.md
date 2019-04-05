# 模块作用

该模块的作用是生成智能对账单的相应指标，对应的表为：

* asset_cust_daily_return
* stock_cust_daily_holding
* stock_cust_daily_return
* stock_cust_return_by_prd
* stock_cust_return_by_prd_by_month
* stock_cust_return_by_prd_ind
* stock_cust_return_by_prd_ind_by_month
* stock_return_by_month

# 运行方式

打包好之后，运行的命令行如下：

```
python auto_run.py <env> <mode> <busi_date>
```

* env表示运行环境，有两种选择分别为“local”：本地运行；“cluster”：集群上运行。
* mode表示运行模式，有两种选择分别为“init”：数据初始化，即根据日期做最初的初始化；“update”：更新给定日期的指标情况。
* busi_date表示运行日期，其格式为“2018-06-10”。