DROP INDEX yf_stock_daily_ticker_day_unique;
CREATE UNIQUE INDEX yf_stock_daily_ticker_day_unique ON yf_stock_daily(upper(ticker), day, dividends);
