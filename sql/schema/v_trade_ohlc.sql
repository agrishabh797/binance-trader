CREATE OR REPLACE VIEW V_TRADE_OHLC AS
WITH RAW_DATA_CTE AS (
SELECT 
    SYMBOL,
    DATE(timestamp) as trade_date,
    MIN(timestamp) AS min_time,
    MAX(timestamp) AS max_time,
    MIN(ltp) as low,
    MAX(ltp) as high
FROM historical_trade_data
GROUP BY SYMBOL, trade_date
)
SELECT 
    rdc.SYMBOL,
    trade_date,
    min(htd1.ltp) as OPEN,
    rdc.high,
    rdc.low,
    max(htd2.ltp) as CLOSE
from RAW_DATA_CTE rdc
join historical_trade_data htd1 on rdc.min_time = htd1.timestamp and rdc.SYMBOL = htd1.SYMBOL 
join historical_trade_data htd2 on rdc.max_time = htd2.timestamp and rdc.SYMBOL = htd2.SYMBOL
group by rdc.SYMBOL, trade_date, rdc.high, rdc.low;
