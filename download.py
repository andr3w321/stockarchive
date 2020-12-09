from argparse import ArgumentParser
import stockarchivedb as sa
import pandas as pd
from model import conn, session
import sys


argparser = ArgumentParser()

## time inputs
_ = argparser.add_argument('--if-market-open', action='store_true', dest="if_market_open", help='run if market is open, exit otherwise', required=False)
_ = argparser.add_argument('--scrape-time', action='store', dest="scrape_time", help='specify the approximate time that we begin scraping options data', required=False)

## ticker inputs
_ = argparser.add_argument('--ticker', action='store', dest="ticker", help='specify the ticker', required=False)
_ = argparser.add_argument('--top', action='store', dest="top", help='input an integer for the amount of top tickers by marketcap you would like to fetch', required=False)
_ = argparser.add_argument('--skip', action='store', dest="skip", help='input an integer for the amount of top tickers by marketcap you would like to skip fetching', required=False)
_ = argparser.add_argument('--sp500', action='store_true', dest="sp500", help='set to true if want to fetch the sp500 tickers', required=False)
_ = argparser.add_argument('--custom-etf-tickers', action='store_true', dest="custom_etf_tickers", help='set to true if want to fetch the custom ETF tickers', required=False)
_ = argparser.add_argument('--futures', action='store_true', dest="futures", help='set to true if want to fetch the yahoo futures tickers', required=False)

## import functions
_ = argparser.add_argument('--import-all-finviz-tickers', action='store_true', dest="import_all_finviz_tickers", help='import all finviz tickers', required=False)
_ = argparser.add_argument('--import-sp500-finviz-tickers', action='store_true', dest="import_sp500_finviz_tickers", help='import sp500 finviz tickers', required=False)
_ = argparser.add_argument('--import-yf-daily-max-history', action='store_true', dest="import_yf_daily_max_history", help='import OHLC max history for provided tickers', required=False)
_ = argparser.add_argument('--import-yf-one-min-bars', action='store_true', dest="import_yf_one_min_bars", help='import OHLC one min bars for provided tickers', required=False)
_ = argparser.add_argument('--import-yf-options', action='store_true', dest="import_yf_options", help='import yahoo finance options for given tickers', required=False)
args = argparser.parse_args()

if args.if_market_open is True:
    market_is_open = sa.is_market_open_today()
    if market_is_open is False:
        # TODO remove print, but for now we'll leave it
        print("Market closed", sa.get_today_string())
        sys.exit()

### ticker inputs
tickers = []
if args.ticker is not None:
    tickers += args.ticker.split(',')
if args.top is not None:
    skip = 0
    if args.skip is not None:
        skip = args.skip
    tickers += sa.get_top_tickers(args.top, skip)
if args.sp500 is True:
    tickers += sa.get_sp500_tickers()
if args.custom_etf_tickers is True:
    tickers += sa.get_custom_etf_tickers()
if args.futures is True:
    tickers += sa.get_yf_futures_tickers()

#print(tickers)

### import functions
if args.import_all_finviz_tickers is True:
    sa.import_all_finviz_tickers()
if args.import_sp500_finviz_tickers is True:
    sa.import_sp500_finviz_tickers()
if args.import_yf_daily_max_history is True:
    for ticker in tickers:
        sa.import_yf_daily_max_history(ticker)
if args.import_yf_one_min_bars is True:
    for ticker in tickers:
        sa.import_yf_one_min_bars(ticker)
if args.import_yf_options is True:
    if args.scrape_time is None:
        raise ValueError("--scrape-time is required if want to scrape yf-options")
    else:
        risk_free_rate = sa.get_risk_free_rate()
        req_count = 0
        for ticker in tickers:
            req_count += sa.import_yf_options(ticker, args.scrape_time, risk_free_rate)
        print(sa.get_today_string(), args.scrape_time, req_count, "requests")

session.commit()
conn.close()
