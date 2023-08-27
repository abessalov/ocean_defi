-----------------------
-- 1. Collecting of all daily aave v2 and v3 transactions
    with 
    supply as (
        select 
            date_trunc('day', evt_block_time) as day,
            case when onBehalfOf in (0xab78997f9656fe933f57b3a616d4366a0e4c5305,
                                        0xd1ba45a0d2370cc04b00e95fc820ff4014150260,
                                        0x57d20c946a7a3812a7225b881cdcd8431d23431c,
                                        0x098256c06ab24f5655c5506a6488781bd711c14b,
                                        0x917eb423a9c70ba2e71b4b3bbc709e74bcca4c03,
                                        0x9600a48ed0f931d0c422d574e3275a90d8b22745,
                                        0xa7e0e66f38b8ad8343cff67118c1f33e827d1455,
                                        0x33333aea097c193e66081e930c33020272b33333,
                                        0x0000cd00001700b10049dfc947103e00e1c62683,
                                        0xfafeb6c262517d7d1171a3ba836e3e2d2897835f)
            then user else onBehalfOf end as user, -- some contracts replacing 
            reserve as token,
            'supply' as event,
            sum(amount) as amt,
            count(*) as cnt
        from aave_v3_ethereum.Pool_evt_Supply
        group by 1,2,3
            union all
        select
            date_trunc('day', evt_block_time) as day,
            case when onBehalfOf in (0xab78997f9656fe933f57b3a616d4366a0e4c5305,
                                        0xd1ba45a0d2370cc04b00e95fc820ff4014150260,
                                        0x57d20c946a7a3812a7225b881cdcd8431d23431c,
                                        0x098256c06ab24f5655c5506a6488781bd711c14b,
                                        0x917eb423a9c70ba2e71b4b3bbc709e74bcca4c03,
                                        0x9600a48ed0f931d0c422d574e3275a90d8b22745,
                                        0xa7e0e66f38b8ad8343cff67118c1f33e827d1455,
                                        0x33333aea097c193e66081e930c33020272b33333,
                                        0x0000cd00001700b10049dfc947103e00e1c62683,
                                        0xfafeb6c262517d7d1171a3ba836e3e2d2897835f)
            then user else onBehalfOf end as user, -- some contracts replacing 
            reserve as token,
            'supply' as event,
            sum(amount) as amt,
            count(*) as cnt
        from aave_v2_ethereum.LendingPool_evt_Deposit
        group by 1,2,3
    )
    , withdraw as (       
        select 
            date_trunc('day', evt_block_time) as day,
            user as user,
            reserve as token,
            'withdraw' as event,
            sum(amount) as amt,
            count(*) as cnt
        from aave_v3_ethereum.Pool_evt_Withdraw
        group by 1,2,3
            union all
        select 
            date_trunc('day', evt_block_time) as day,
            user as user,
            reserve as token,
            'withdraw' as event,
            sum(amount) as amt,
            count(*) as cnt
        from aave_v2_ethereum.LendingPool_evt_Withdraw
        group by 1,2,3
    )
    , borrow as (
        select 
            date_trunc('day', evt_block_time) as day,
            case when onBehalfOf in (0xab78997f9656fe933f57b3a616d4366a0e4c5305,
                                        0xd1ba45a0d2370cc04b00e95fc820ff4014150260,
                                        0x57d20c946a7a3812a7225b881cdcd8431d23431c,
                                        0x098256c06ab24f5655c5506a6488781bd711c14b,
                                        0x917eb423a9c70ba2e71b4b3bbc709e74bcca4c03,
                                        0x9600a48ed0f931d0c422d574e3275a90d8b22745,
                                        0xa7e0e66f38b8ad8343cff67118c1f33e827d1455,
                                        0x33333aea097c193e66081e930c33020272b33333,
                                        0x0000cd00001700b10049dfc947103e00e1c62683,
                                        0xfafeb6c262517d7d1171a3ba836e3e2d2897835f)
            then user else onBehalfOf end as user, -- some contracts replacing 
            reserve as token,
            'borrow' as event,
            sum(amount) as amt,
            count(*) as cnt
        from aave_v3_ethereum.Pool_evt_Borrow
        group by 1,2,3
            union all
        select 
            date_trunc('day', evt_block_time) as day,
            case when onBehalfOf in (0xab78997f9656fe933f57b3a616d4366a0e4c5305,
                                        0xd1ba45a0d2370cc04b00e95fc820ff4014150260,
                                        0x57d20c946a7a3812a7225b881cdcd8431d23431c,
                                        0x098256c06ab24f5655c5506a6488781bd711c14b,
                                        0x917eb423a9c70ba2e71b4b3bbc709e74bcca4c03,
                                        0x9600a48ed0f931d0c422d574e3275a90d8b22745,
                                        0xa7e0e66f38b8ad8343cff67118c1f33e827d1455,
                                        0x33333aea097c193e66081e930c33020272b33333,
                                        0x0000cd00001700b10049dfc947103e00e1c62683,
                                        0xfafeb6c262517d7d1171a3ba836e3e2d2897835f)
            then user else onBehalfOf end as user, -- some contracts replacing 
            reserve as token,
            'borrow' as event,
            sum(amount) as amt,
            count(*) as cnt
        from aave_v2_ethereum.LendingPool_evt_Borrow
        group by 1,2,3
    )
    , repay as (
        select 
            date_trunc('day', evt_block_time) as day,
            user as user,
            reserve as token,
            'repay' as event,
            sum(amount) as amt,
            count(*) as cnt
        from aave_v3_ethereum.Pool_evt_Repay
        group by 1,2,3
            union all
        select 
            date_trunc('day', evt_block_time) as day,
            user as user,
            reserve as token,
            'repay' as event,
            sum(amount) as amt,
            count(*) as cnt
        from aave_v2_ethereum.LendingPool_evt_Repay
        group by 1,2,3
    )
    , liquidate_deposit as (
        select 
            date_trunc('day', evt_block_time) as day,
            user as user,
            collateralAsset as token,
            'liquidate_deposit' as event,
            sum(liquidatedCollateralAmount) as amt,
            count(*) as cnt
        from aave_v3_ethereum.Pool_evt_LiquidationCall
        group by 1,2,3
            union all
        select 
            date_trunc('day', evt_block_time) as day,
            user as user,
            collateralAsset as token,
            'liquidate_deposit' as event,
            sum(liquidatedCollateralAmount) as amt,
            count(*) as cnt
        from aave_v2_ethereum.LendingPool_evt_LiquidationCall
        group by 1,2,3
    )
    , liquidate_borrow as (
        select 
            date_trunc('day', evt_block_time) as day,
            user,
            debtAsset as token,
            'liquidate_borrow' as event,
            sum(debtToCover) as amt,
            count(*) as cnt
        from aave_v3_ethereum.Pool_evt_LiquidationCall
        group by 1,2,3
            union all
        select 
            date_trunc('day', evt_block_time) as day,
            user as user,
            collateralAsset as token,
            'liquidate_deposit' as event,
            sum(liquidatedCollateralAmount) as amt,
            count(*) as cnt
        from aave_v2_ethereum.LendingPool_evt_LiquidationCall
        group by 1,2,3
    )
    , trnx_daily as (
        select * from supply
            union all 
        select * from withdraw
            union all 
        select * from borrow
            union all 
        select * from repay
            union all 
        select * from liquidate_deposit
            union all 
        select * from liquidate_borrow
    )
    , tokens as (
        select distinct 
            token,
            b.symbol,
            b.decimals
        from trnx_daily a
        left join tokens.erc20 b on 
            a.token = b.contract_address
        where blockchain = 'ethereum' 
    )
    , trnx_daily_agg as (
        select 
            a1.user,
            a1.token,
            a2.symbol,
            a1.day,
            coalesce(a1.cnt_supply, 0) as cnt_supply,
            coalesce(a1.cnt_withdraw, 0) as cnt_withdraw,
            coalesce(a1.cnt_liquidate_supply, 0) as cnt_liquidate_supply,
            coalesce(a1.cnt_borrow, 0) as cnt_borrow,
            coalesce(a1.cnt_repay, 0) as cnt_repay,
            coalesce(a1.cnt_liquidate_borrow, 0) as cnt_liquidate_borrow,
            coalesce(a1.amt_supply / power(10,a2.decimals), 0) as amt_supply,
            coalesce(a1.amt_withdraw / power(10,a2.decimals), 0) as amt_withdraw,
            coalesce(a1.amt_liquidate_supply / power(10,a2.decimals), 0) as amt_liquidate_supply,
            coalesce(a1.amt_borrow / power(10,a2.decimals), 0) as amt_borrow,
            coalesce(a1.amt_repay / power(10,a2.decimals), 0) as amt_repay,
            coalesce(a1.amt_liquidate_borrow / power(10,a2.decimals), 0) as amt_liquidate_borrow
        from (
            select 
                user,
                token,
                day,
                sum(case when event = 'supply' then cnt else null end) as cnt_supply,
                sum(case when event = 'withdraw' then cnt else null end) as cnt_withdraw,
                sum(case when event = 'liquidate_deposit' then cnt else null end) as cnt_liquidate_supply,
                sum(case when event = 'borrow' then cnt else null end) as cnt_borrow,
                sum(case when event = 'repay' then cnt else null end) as cnt_repay,
                sum(case when event = 'liquidate_borrow' then cnt else null end) as cnt_liquidate_borrow,

                sum(case when event = 'supply' then amt else null end) as amt_supply,
                sum(case when event = 'withdraw' then amt else null end) as amt_withdraw,
                sum(case when event = 'liquidate_deposit' then amt else null end) as amt_liquidate_supply,
                sum(case when event = 'borrow' then amt else null end) as amt_borrow,
                sum(case when event = 'repay' then amt else null end) as amt_repay,
                sum(case when event = 'liquidate_borrow' then amt else null end) as amt_liquidate_borrow
            from trnx_daily
            group by 1,2,3
        ) a1 
        left join tokens a2 on
            a1.token = a2.token
    )


-----------------------
-- 2. Creating interests rates table 
    , rates as (
        select 
            reserve,
            date_trunc('day', evt_block_time) AS day,
            avg(liquidityRate/1e27) as rate_supply,
            avg(stableBorrowRate/1e27) as rate_borrow_stable,
            avg(variableBorrowRate/1e27) as rate_borrow_var
        from (
            select reserve, evt_block_time, liquidityRate, stableBorrowRate, variableBorrowRate from aave_v3_ethereum.Pool_evt_ReserveDataUpdated
                union all
            select reserve, evt_block_time, liquidityRate, stableBorrowRate, variableBorrowRate from aave_v2_ethereum.LendingPool_evt_ReserveDataUpdated
        )
        group by 1,2
    )
    , rates2 as (
        select 
            a.*,
            b.rate_supply,
            b.rate_borrow_stable,
            b.rate_borrow_var
        from (
            select 
                a1.reserve,
                a2.day
            from (select distinct reserve from rates) a1
            cross join (select distinct day from rates) a2
        ) a
        left join rates b on 
            a.reserve = b.reserve
            and a.day = b.day
    )
    -- forward fill of rates
    , rates_fill as (
        select 
            a.reserve,
            a.day,
            first_value(rate_supply) over(partition by reserve, val_count order by day) as rate_supply,
            first_value(rate_borrow_stable) over(partition by reserve, val_count order by day) as rate_borrow_stable,
            first_value(rate_borrow_var) over(partition by reserve, val_count order by day) as rate_borrow_var
        from (
            select 
                *,    
                count(rate_supply) over(partition by reserve order by day) as val_count
            from rates2
        ) a
    )


-----------------------
-- 3. Creating token exchange rates table 
    , exchange1 as (
        select 
            contract_address,
            date_trunc('day', minute) AS day,
            avg(price) as token_price
        from prices.usd
        where blockchain = 'ethereum'
            and contract_address in (select distinct token from tokens)
        group by 1,2
    )
    , exchange2 as (
        select 
            token_bought_address as contract_address,
            date_trunc('day', block_time) as day,
            sum(amount_usd) / sum(token_bought_amount) as token_price
        from dex.trades
        where blockchain = 'ethereum'
            and token_bought_address in (select distinct token from tokens)
        group by 1,2
    )
    , exchange_all as (
        select 
            b.*,
            row_number() over(partition by contract_address,day_last order by day) as rn
        from (
            select 
                a.contract_address,
                a.day,
                date_trunc('month', a.day) + interval '1' month - interval '1' day as day_last,
                date_trunc('month', a.day) as day_first,
                avg(a.token_price) as token_price_avg
            from (
                select * from exchange1
                    union all
                select * from exchange2 
                where token_price is not null
            ) a
            group by 1,2,3
        ) b 
    )
    -- last not null value in the month
    , exchange_monthly as (
        select
            a.contract_address,
            a.day_last as day,
            a.token_price_avg as token_price
        from exchange_all a
        join (
            select 
                contract_address,
                day_last,
                max(rn) as rn_max
            from exchange_all 
            group by 1,2
        ) b on 
            a.contract_address = b.contract_address
            and a.day_last = b.day_last
            and a.rn = b.rn_max
        order by 1,2
    )


-----------------------
-- 4. Creating the table with all unique days starting from the first user's transaction by every token and filling that table 
    , days as (
        select 
            d1.day,
            date_trunc('month', d1.day) + interval '1' month - interval '1' day as day_last,
            date_trunc('month', d1.day) as day_first
        from (select distinct day from trnx_daily_agg) d1
        order by 1
    )
    , user as (
        select
            user,
            min(day) as day_start
        from trnx_daily_agg
        group by 1
    )
    , user_token as (
        select
            user,
            token,
            min(day) as day_start_token
        from trnx_daily_agg a
        group by 1,2
    )
    , user_token_main as (
        select 
            a1.user,
            a4.day_start,
            a1.token,
            a1.day_start_token,
            a1.day,
            a1.day_last,
            a1.day_first,
            row_number() over(partition by a1.user, a1.token order by a1.day) as day_number,
            a3.rate_supply,
            a3.rate_borrow_stable,
            a3.rate_borrow_var,
            
            a2.cnt_supply,
            a2.cnt_withdraw,
            a2.cnt_liquidate_supply,
            a2.cnt_borrow,
            a2.cnt_repay,
            a2.cnt_liquidate_borrow,
            a2.amt_supply,
            a2.amt_withdraw,
            a2.amt_liquidate_supply,
            a2.amt_borrow,
            a2.amt_repay,
            a2.amt_liquidate_borrow,

            sum(a2.cnt_supply) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as cnt_supply_monthly,
            sum(a2.cnt_withdraw) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as cnt_withdraw_monthly,
            sum(a2.cnt_liquidate_supply) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as cnt_liquidate_supply_monthly,
            sum(a2.cnt_borrow) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as cnt_borrow_monthly,
            sum(a2.cnt_repay) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as cnt_repay_monthly,
            sum(a2.cnt_liquidate_borrow) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as cnt_liquidate_borrow_monthly,

            sum(a2.amt_supply) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as amt_supply_monthly,
            sum(a2.amt_withdraw) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as amt_withdraw_monthly,
            sum(a2.amt_liquidate_supply) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as amt_liquidate_supply_monthly,
            sum(a2.amt_borrow) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as amt_borrow_monthly,
            sum(a2.amt_repay) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as amt_repay_monthly,
            sum(a2.amt_liquidate_borrow) over (partition by a1.user, a1.token, a1.day_last order by a1.day) as amt_liquidate_borrow_monthly,

            sum(a2.amt_supply - a2.amt_withdraw - a2.amt_liquidate_supply) over(partition by a1.user, a1.token order by a1.day) as amt_supply_cumsum,
            sum(if(a2.cnt_supply + a2.cnt_withdraw + a2.cnt_liquidate_supply > 0, 1, 0)) over(partition by a1.user, a1.token order by a1.day) as count_supply,
            sum(a2.amt_borrow - a2.amt_repay - a2.amt_liquidate_borrow) over(partition by a1.user, a1.token order by a1.day) as amt_borrow_cumsum,
            sum(if(a2.cnt_borrow + a2.cnt_repay + a2.cnt_liquidate_borrow > 0, 1, 0)) over(partition by a1.user, a1.token order by a1.day) as count_borrow
        from (
            select
                t1.user,
                t1.token,
                t1.day_start_token,
                d1.day,
                d1.day_last,
                d1.day_first
            from user_token t1
            cross join days d1
            where d1.day >= t1.day_start_token
        ) a1
        left join trnx_daily_agg a2 on
            a1.user = a2.user
            and a1.token = a2.token
            and a1.day = a2.day
        left join rates_fill a3 on 
            a1.token = a3.reserve
            and a1.day = a3.day
        left join user a4 on 
            a1.user = a4.user
        order by 1,3,5
    )
    , user_token_main2 as (
        select
            a.*,
            coalesce(b.token_price, 5.6) as token_price, -- replace null renFIL token values for the March 2023
            cnt_supply_monthly + cnt_withdraw_monthly + cnt_liquidate_supply_monthly + cnt_borrow_monthly + cnt_repay_monthly as cnt_trnx,
            if(amt_supply_cumsum > 0 or amt_borrow_cumsum > 0 or interest_supply_monthly > 0 or interest_borrow_monthly > 0, 1, 0) as active_record
        from (
            select 
                a1.user,
                a1.day_start,
                a1.token,
                a1.day_start_token,
                a1.day,
                a1.day_last,
                a1.day_number,
                     
                coalesce(a1.cnt_supply_monthly, 0) as cnt_supply_monthly,
                coalesce(a1.cnt_withdraw_monthly, 0) as cnt_withdraw_monthly,
                coalesce(a1.cnt_liquidate_supply_monthly, 0) as cnt_liquidate_supply_monthly,
                coalesce(a1.cnt_borrow_monthly, 0) as cnt_borrow_monthly,
                coalesce(a1.cnt_repay_monthly, 0) as cnt_repay_monthly,
                coalesce(a1.cnt_liquidate_borrow_monthly, 0) as cnt_liquidate_borrow_monthly,

                coalesce(a1.amt_supply_monthly, 0) as amt_supply_monthly,
                coalesce(a1.amt_withdraw_monthly, 0) as amt_withdraw_monthly,
                coalesce(a1.amt_liquidate_supply_monthly, 0) as amt_liquidate_supply_monthly,
                coalesce(a1.amt_borrow_monthly, 0) as amt_borrow_monthly,
                coalesce(a1.amt_repay_monthly, 0) as amt_repay_monthly,
                coalesce(a1.amt_liquidate_borrow_monthly, 0) as amt_liquidate_borrow_monthly,

                if(a1.amt_supply_cumsum < 0, 0, a1.amt_supply_cumsum) as amt_supply_cumsum,
                if(a1.amt_borrow_cumsum < 0, 0, a1.amt_borrow_cumsum) as amt_borrow_cumsum,

                sum(if(a1.amt_supply_cumsum < 0, 0, a1.amt_supply_cumsum) * a1.rate_supply / 365) over (partition by a1.user, a1.token, a1.day_last) as interest_supply_monthly,
                sum(if(a1.amt_borrow_cumsum < 0, 0, a1.amt_borrow_cumsum) * a1.rate_borrow_stable / 365) over (partition by a1.user, a1.token, a1.day_last) as interest_borrow_monthly
            from user_token_main a1
        ) a
        left join exchange_monthly b on 
            a.token = b.contract_address
            and a.day = b.day
        where a.day = a.day_last      
    ) 


-----------------------
-- 5. Aggregating previous table on the user level (considering all his tokens together)
    , user_main as (
        select
            user,
            day_start,
            day_last as report_month,

            count(distinct token) as cnt_tokens,
            count(distinct case when cnt_supply_monthly > 0 then token else null end) as cnt_tokens_supply,
            count(distinct case when cnt_borrow_monthly > 0 then token else null end) as cnt_tokens_borrow,

            sum(cnt_supply_monthly) as cnt_supply,
            sum(cnt_withdraw_monthly) as cnt_withdraw,
            sum(cnt_liquidate_supply_monthly) as cnt_liquidate_supply,
            sum(cnt_borrow_monthly) as cnt_borrow,
            sum(cnt_repay_monthly) as cnt_repay,
            sum(cnt_liquidate_borrow_monthly) as cnt_liquidate_borrow,
            
            sum(token_price * amt_supply_monthly) as amt_supply,
            sum(token_price * amt_withdraw_monthly) as amt_withdraw,
            sum(token_price * amt_liquidate_supply_monthly) as amt_liquidate_supply,
            sum(token_price * amt_borrow_monthly) as amt_borrow,
            sum(token_price * amt_repay_monthly) as amt_repay,
            sum(token_price * amt_liquidate_borrow_monthly) as amt_liquidate_borrow,

            sum(token_price * amt_supply_cumsum) as current_supply,
            sum(token_price * amt_borrow_cumsum) as current_borrow,

            sum(token_price * interest_supply_monthly) as interest_supply,
            sum(token_price * interest_borrow_monthly) as interest_borrow

        from user_token_main2
        group by 1,2,3
        having sum(cnt_trnx) + sum(active_record) > 0

    ) 

-- ** Note: there may be some inaccuracies in the calculation of interests and the current monthly values of supply and borrow

select 
    *
from user_main
-- where amt_supply is null
order by 1,3
