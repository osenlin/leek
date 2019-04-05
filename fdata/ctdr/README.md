# 模块作用

该模块的作用是计算每日交易（也就是将每日每支股票的所有买入视为一次交易）的收益和收益率，它负责生成的表有如下几张：

* stock_close_tradebyday_long_data
* stock_close_tradebyday_short_data
* stock_unclose_tradebyday_long_data
* stock_unclose_tradebyday_short_data
* stock_unclose_tradebyday_long_cal
* stock_unclose_tradebyday_short_cal

# 运行方式

打包好之后，运行的命令行如下：

```
python auto_run.py <env> <mode> <busi_date>
```

* env表示运行环境，有两种选择分别为“local”：本地运行；“cluster”：集群上运行。
* mode表示运行模式，有两种选择分别为“init”：数据初始化，即根据日期做最初的初始化；“update”：更新给定日期的指标情况。
* busi_date表示运行日期，其格式为“2018-06-10”。

# 运行工具
跑一段时间内的数据，提供了俩种方式
```
python batch_task.py startdate enddate single_or_multi num_parallel
```
* startdate:任务开始时间，其格式为“2018-06-10”。
* enddate:任务结束时间，其格式为“2018-06-10”。
* single_or_multi:单进程/多进程，1表示单进程，非1表示多进程。
* num_parallel:任务的并行度，在single_or_multi标为多进程状态才会启用。