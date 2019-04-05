        print u"""
        select
            nvl(ffc.trade_id,d.trade_id) trade_id,
            nvl(asset,0) asset,
            nvl(debt,0) debt,
            nvl(now_net_asset,0) net_asset,
            nvl(capital_in,0) capital_in,
            nvl(capital_out,0) capital_out,
            nvl(pre_net_asset,0) + nvl(capital_in,0) + nvl(capital_out,0) – nvl(now_net_asset,0) return,
            case when (nvl(pre_asset,0) + nvl(pre_debt,0) + nvl(capital_in,0))> (-1 * nvl(pre_debt,0) + nvl(capital_in,0))
                then nvl(pre_asset,0) + nvl(pre_debt,0) + nvl(capital_in,0)
                else -1 * nvl(pre_debt,0) + nvl(capital_in,0)
            end  inv,
        from (
            select nvl(fa.trade_id,c.trade_id) trade_id,nvl(asset,0) asset,
                    nvl(debt,0) debt,nvl(capital_in,0) capital_in,nvl(capital_out,0) capital_out
            from (
                select nvl(a.trade_id,b.trade_id) trade_id,nvl(a.mkt_val,0) asset,nvl(b.mkt_val,0) debt from (
                    select trade_id, sum(mkt_val) mkt_val
                    from odata.stock_asset_holding
                    where busi_date='2017-03-17'
                    group by trade_id
                ) a full outer join (
                    select trade_id, sum(mkt_val) mkt_val
                    from odata.stock_debt_holding
                    where busi_date=''
                    group by trade_id
                ) b on a.trade_id=b.trade_id
            ) fa full outer join(
                select trade_id,sum(capital_in) capital_in,sum(capital_out) capital_out
                from fdata.stock_cash_flow
                where busi_date='2017-03-17'
                group by trade_id
            ) c on fa.trade_id=c.trade_id
        ) ffc full outer join (
            select trade_id,
                sum(case when busi_date='startdate' then net_asset else 0 end) pre_net_asset,
                sum(case when busi_date='enddate' then net_asset else 0 end) now_net_asset,
                sum(case when busi_date='start' then asset else 0 end) pre_asset,
                sum(case when busi_date='start' then debt else 0 end) pre_debt,
            from adata.asset_cust_daily_return
            where busi_date in('startdate','enddate')
            group by trade_id
        ) d left outer join(
            select trade_id,max(qty_exception),max(return_rate_exception)
            group by trade_id
        ) e
        """