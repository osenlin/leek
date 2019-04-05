# 开发流程

master分支为发布分支，develop分支为开发分支；每次开发的时候

* 从develop分支folk出一个新的分支，比如feature_xxx
* 开发调试完成之后，提交pr从feature_xxx到develop

# 系统架构

整个项目是基于PYSPARK2和Spark on HIVE技术搭建而成

# 系统结构

该系统分为若干个子系统，每个子系统负责生成一系列的量化指标。具体地结构如下，每个模块的运行方式和具体说明，请见子模块的README

* leek
	* fdata：负责生成fdata数据库里面的指标
		* csr：清仓股票收益模块。csr表示的是close stock return
		* ctdr：每日交易的收益和收益率（每日每支股票的所有买入视为一次交易，并以此计算交易的收益和收益率）。ctdr表示的是close trade by day return
		* cotr：每笔开仓交易的收益和收益率（做多情况下，每次买入视为一次交易，做空情况下，每次卖出视为一次交易）。cotr表示close open trade return
		* cctr：每笔闭仓交易的收益和收益率（做多情况下，每次卖出视为一次交易，做空情况下，每次买入视为一次交易）。cctr表示close close trade return
		* dc：数据核对，包括合并现金流，账目核对以及相应的统计和预警。dc表示data check（todo，需要拆分）
	* adata：负责生成adata数据库里面的指标
		* sbs：生成智能对账单相应的指标。sbs表示smart balance sheet
		* ssid：生成账户诊断相应的指标。sid表示smart stock investment diagnose
	* common：存放通用的函数

# 打包和运行方式

* 运行./package.sh进行打包
* 以fdata/csr为例，运行各个子模块的方法是

```
python target/fdata/csr/bin/auto_run.py <env> <mode> <busi_date>
```
* 部署的时候，只需将target目录下的东西拷贝到客户的服务器上即可