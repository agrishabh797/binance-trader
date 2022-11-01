DROP TABLE historical_trade_data CASCADE;
CREATE TABLE historical_trade_data (
	symbol varchar(200),
	timestamp timestamp, 
	ltp numeric, 
	ltq numeric, 
	oi numeric, 
	bid numeric, 
	bid_qty numeric, 
	ask numeric, 
	ask_qty numeric,
	expiry_date date
);

CREATE INDEX idx_symbol
ON historical_trade_data (symbol);

CREATE INDEX idx_timestamp
ON historical_trade_data ((timestamp::DATE));
