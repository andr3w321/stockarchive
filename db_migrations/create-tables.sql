SET TIME ZONE 'UTC';

DROP TABLE IF EXISTS yf_stock_daily;
CREATE TABLE yf_stock_daily(
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  ticker VARCHAR(30),
  day DATE,
  open NUMERIC(10,4),
  high NUMERIC(10,4),
  low NUMERIC(10,4),
  close NUMERIC(10,4),
  volume BIGINT,
  dividends NUMERIC(10,4),
  stock_splits INTEGER
);

CREATE UNIQUE INDEX yf_stock_daily_ticker_day_unique ON yf_stock_daily(upper(ticker), day);
CREATE INDEX yf_stock_daily_ticker_idx ON yf_stock_daily(upper(ticker));
CREATE INDEX yf_stock_daily_day ON yf_stock_daily(day);

DROP TABLE IF EXISTS yf_stock_intraday;
CREATE TABLE yf_stock_intraday(
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  ticker VARCHAR(30),
  bar_start TIMESTAMP WITH TIME ZONE,
  open NUMERIC(10,4),
  high NUMERIC(10,4),
  low NUMERIC(10,4),
  close NUMERIC(10,4),
  volume BIGINT,
  dividends NUMERIC(10,4),
  stock_splits INTEGER,
  rth INTEGER
);

CREATE UNIQUE INDEX yf_stock_intraday_ticker_bar_start_unique ON yf_stock_intraday(upper(ticker), bar_start);
CREATE INDEX yf_stock_intraday_ticker_idx ON yf_stock_intraday(upper(ticker));
CREATE INDEX yf_stock_intraday_bar_start ON yf_stock_intraday(bar_start);

DROP TABLE IF EXISTS yf_option;
CREATE TABLE yf_option (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  ticker VARCHAR(30),
  is_call INTEGER,
  contract_symbol VARCHAR(80),
  expiration TIMESTAMP WITH TIME ZONE,
  scrape_day DATE,
  scrape_time VARCHAR(30),
  last_trade_date TIMESTAMP WITH TIME ZONE,
  strike INTEGER,
  last_price NUMERIC(10,4),
  underlying_bid NUMERIC(10,4),
  underlying_ask NUMERIC(10,4),
  bid NUMERIC(10,4),
  ask NUMERIC(10,4),
  volume BIGINT,
  open_interest INTEGER,
  yf_implied_vol NUMERIC(10,4),
  itm INTEGER,
  atm INTEGER,
  implied_vol NUMERIC(10,4),
  delta NUMERIC(10,4),
  gamma NUMERIC(10,4),
  theta NUMERIC(10,4),
  vega NUMERIC(10,4)
);

CREATE UNIQUE INDEX yf_option_symbol_scrape_time_unique ON yf_option(upper(ticker), contract_symbol, scrape_day, upper(scrape_time));
CREATE INDEX yf_option_ticker_idx ON yf_option(upper(ticker));
CREATE INDEX yf_option_scrape_day_idx ON yf_option(scrape_day);
CREATE INDEX yf_option_scrape_time_idx ON yf_option(upper(scrape_time));
CREATE INDEX yf_option_expiration_idx ON yf_option(expiration);
CREATE INDEX yf_option_contract_symbol_idx ON yf_option(contract_symbol);

DROP TABLE IF EXISTS fv_stock_info;
CREATE TABLE fv_stock_info(
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  ticker VARCHAR(30),
  company VARCHAR(180),
  sector VARCHAR(80),
  industry VARCHAR (180),
  country VARCHAR(80)
);

CREATE UNIQUE INDEX fv_stock_info_unique ON fv_stock_info(upper(ticker));
CREATE INDEX fv_stock_info_ticker_idx ON fv_stock_info(upper(ticker));

DROP TABLE IF EXISTS fv_stock_daily;
CREATE TABLE fv_stock_daily(
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  ticker VARCHAR(30),
  day DATE,
  n INTEGER,
  marketcap NUMERIC(14,4),
  pe NUMERIC(10,2),
  price NUMERIC(10,2),
  change NUMERIC(10,2),
  volume BIGINT
);

CREATE UNIQUE INDEX fv_stock_daily_unique ON fv_stock_daily(upper(ticker), day);
CREATE INDEX fv_stock_daily_ticker_idx ON fv_stock_daily(upper(ticker));
CREATE INDEX fv_stock_daily_day_idx ON fv_stock_daily(day);

DROP TABLE IF EXISTS ticker_group;
CREATE TABLE ticker_group(
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
  start_day DATE,
  tag VARCHAR(30),
  tickers VARCHAR(30)[]
);

/* Automatically update updated_at field */
CREATE OR REPLACE FUNCTION update_updated_at_column()	
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;	
END;
$$ language 'plpgsql';

CREATE TRIGGER update_fv_stock_info_updated_at BEFORE UPDATE ON fv_stock_info FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
