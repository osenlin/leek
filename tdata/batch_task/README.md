# 模块作用

该模块的作用是节省时间

* 更新的表
* asset_cust_daily_return
* stock_cash_flow
* stock_daily_check_data
* stock_daily_check_report
* stock_cust_daily_return
* stock_cust_daily_holding

打包好之后，运行的命令行如下：

```
python auto_run.py <env> <mode> <start_date> <end_date>
```

* env表示运行环境，有两种选择分别为“local”：本地运行；“cluster”：集群上运行。
* mode表示运行模式，有两种选择分别为“init”：数据初始化，即根据日期做最初的初始化；“update”：更新给定日期的指标情况。
* start_date表示运行开始日期，其格式为“2018-06-10”。
* end_date表示运行结束日期，其格式为“2018-06-10”。