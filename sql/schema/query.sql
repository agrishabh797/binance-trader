with cte1 as (
select
    SYMBOL,
    TRADE_DATE,
    CLOSE AS LTP
from V_TRADE_OHLC
    where SYMBOL = 'NIFTY22JANFUT'
),
cte2 as (
select
    SYMBOL,
    TRADE_DATE,
    CLOSE AS LTP
from V_TRADE_OHLC
    where SYMBOL = 'BANKNIFTY22JANFUT'
)
select cte1.trade_date,
cte1.ltp as NIFTY22JANFUT_LTP,
cte2.ltp as BANKNIFTY22JANFUT_LTP,
cte2.ltp - cte1.ltp as change,
round( CAST(float8((cte2.ltp - cte1.ltp) * 100 / cte1.ltp)as numeric), 2) || ' %' as relative_change
from cte1 join cte2 on (cte1.trade_date = cte2.trade_date);