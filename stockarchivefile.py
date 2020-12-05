import finvizlite as fl
import yfinance as yf
import datetime
import requests
import time
import pandas as pd
import os
import json

OUTPUT_PATH = "./data"
YAHOO_BASE_URL = "https://query1.finance.yahoo.com"

### ticker group fetch functions
def get_popular_etfs_tickers():
    return ["SPY","VXX","EEM","EWZ","TLT","HYG","GDX","SLV","KRE"]

def get_spdr_sector_etf_tickers():
    return ["XLC","XLY","XLP","XLE","XLF","XLV","XLI","XLB","XLRE","XLK","XLU"]

def get_custom_etf_tickers():
    return get_popular_etfs_tickers() + get_spdr_sector_etf_tickers()

def get_yf_futures_tickers():
    #all_tickers = ['ES=F','YM=F','NQ=F','RTY=F','ZB=F','ZN=F','ZF=F','ZT=F','GC=F','MGC=F','SI=F','SIL=F','PL=F','HG=F','PA=F','CL=F','HO=F','NG=F','RB=F','BZ=F','ZC=F','ZO=F','KE=F','ZR=F','ZM=F','ZL=F','ZS=F','GF=F','HE=F','LE=F','CC=F','KC=F','CT=F','LB=F','OJ=F','SB=F']
    # all the tickers I'm interested in
    return ['ES=F','NQ=F','RTY=F','ZN=F','ZB=F','GC=F','SI=F','HG=F','CL=F','NG=F','ZC=F','ZO=F','KE=F','ZS=F','HE=F','LE=F','LB=F']

def get_sp500_tickers(sp500csv="{}/finviz/sp500-latest.csv".format(OUTPUT_PATH)):
    """ Get all the sp500 tickers in a list and return it """
    df = pd.read_csv(sp500csv)
    return df['Ticker'].tolist()

def get_top_tickers(n, allcsv="{}/finviz/all-latest.csv".format(OUTPUT_PATH), skip=0):
    """ Get the top n tickers by marketcap """
    df = pd.read_csv(allcsv)
    return df['Ticker'].tolist()[skip:n]

### general functions
def is_market_open(day):
    """ Get the current SPY trading data
    if SPY trading data is for today then the market is open
    else market must be closed today """

    url = "{}/v8/finance/chart/SPY".format(YAHOO_BASE_URL)
    data = requests.get(url)
    json = data.json()
    meta = json['chart']['result'][0]['meta']
    utc_dt = meta['regularMarketTime']
    gmtoffset = meta['gmtoffset']
    local_dt = datetime.datetime.utcfromtimestamp(utc_dt + gmtoffset)
    local_date = local_dt.date()
    if local_date == day:
        return True
    else:
        return False

def is_market_open_today():
    return is_market_open(datetime.date.today())

def get_today_string():
    return str(datetime.date.today())

def download_file(url, filename):
    """ Download large file in chunks."""
    with requests.get(url, stream=True) as res:
        # TODO if status.code == 502 YAHOO API ERROR
        print(res.status_code)
        print(res.text)
        with open(filename, 'wb') as f:
            for chunk in res.iter_content(chunk_size=8192): 
                f.write(chunk)
    return filename

def get_new_json(url, headers={}):
    """ Request a url and return the json data, or return json data with an error code """
    print(url)
    res = requests.get(url, headers)
    if res.status_code == 200:
        return res.json()
    else:
        print("ERROR:", res.status_code, "URL:", url)
        return {"error_code": res.status_code, "error_msg": "URL Error", "url": url}

def save_json(filename, data):
    """ Given a filename and json data, write the json data to disk.
    Also create any necessary directories that don't current exist to the filename path"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)

### download functions
def download_all_finviz_tickers(output_path=OUTPUT_PATH):
    df = fl.scrape_all("https://finviz.com/screener.ashx?v=111&o=-marketcap", print_urls=True)
    df.to_csv("{}/finviz/all-{}.csv".format(output_path, get_today_string()), index=False)
    df.to_csv("{}/finviz/all-latest.csv".format(output_path), index=False)

def download_sp500_finviz_tickers(output_path=OUTPUT_PATH):
    df = fl.scrape_all("https://finviz.com/screener.ashx?v=111&f=idx_sp500&o=-marketcap", print_urls=True)
    df.to_csv("{}/finviz/sp500-{}.csv".format(output_path, get_today_string()), index=False)
    df.to_csv("{}/finviz/sp500-latest.csv".format(output_path), index=False)

def download_yf_daily_max_history(ticker, output_path=OUTPUT_PATH):
    ## raw request methods
    # period1 = -2208988800 # the year 1900
    # period2 = int(time.time()) # now
    # interval = "1d"

    # option 1) use yahoo API download url
    # doesn't show splits or dividends, but might be more friendly to yahoo api rate limits
    # events = "history"
    # include_adjusted_close = "true"
    # url = "{}/v7/finance/download/{}?period1={}&period2={}&interval={}&events={}&includeAdjustedClose={}".format(YAHOO_BASE_URL, ticker, period1, period2, interval, events, include_adjusted_close)
    # download_file(url, "{}/yf/daily/{}-max-history.csv".format(output_path, ticker))

    # option 2) use yahoo API chart url
    # events = "div%2Csplit" # include dividends and splits
    # url = "{}/v8/finance/chart/{}?symbol={}&period1={}&period2={}&interval={}&events={}".format(YAHOO_BASE_URL, ticker, ticker, period1, period2, interval, events)
    # download_file(url, "{}/yf/daily/{}-max-history.json".format(output_path, ticker))

    # yfinance package method
    df = yf.Ticker(ticker).history(period="max", interval="1d", prepost=False, actions=True, auto_adjust=True, back_adjust=False)
    # for daily candles, it should be fine to overrite the existing csv
    df.to_csv("{}/yf/daily/{}-max-history.csv".format(output_path, ticker))

def download_yf_one_min_bars(ticker, output_path=OUTPUT_PATH):
    ## raw request method
    # period1 = int((datetime.datetime.now() - datetime.timedelta(days=7)).timestamp()) # 7 days ago
    # period2 = int(time.time()) # now
    # interval = "1m"
    # include_pre_post = "true"
    # events = "div%2Csplit" # include dividends and splits
    # url = "{}/v8/finance/chart/{}?symbol={}&period1={}&period2={}&interval={}&includePrePost={}&events={}".format(YAHOO_BASE_URL, ticker, ticker, period1, period2, interval, include_pre_post, events)
    # download_file(url, "{}/yf/intraday/{}.csv".format(output_path, ticker))

    # yfinance package method
    df_latest = yf.Ticker(ticker).history(period="5d", interval="1m", prepost=True, actions=True, auto_adjust=True, back_adjust=False)
    # for 1m candles, we should append the new csv to old csv, then remove duplicate rows
    file_dir = "{}/yf/intraday/".format(output_path)
    df_latest.to_csv("{}{}-latest.csv".format(file_dir, ticker))
    filename_main = "{}{}.csv".format(file_dir, ticker)
    if os.path.exists(filename_main):
        # read, append, drop duplicate rows, write
        df_main = pd.read_csv(filename_main)
        df_main.append(df_latest).drop_duplicates().to_csv(filename_main)
    else:
        df_latest.to_csv(filename_main)

def convert_options_json_to_df(options, are_calls, quote):
    options_df = pd.DataFrame(options).reindex(columns=[
        'contractSymbol',
        'lastTradeDate',
        'strike',
        'expiration',
        'lastPrice',
        'bid',
        'ask',
        'change',
        'percentChange',
        'volume',
        'openInterest',
        'impliedVolatility',
        'inTheMoney',
        'contractSize',
        'currency'])
    if are_calls == 1:
        options_df['is_call'] = 1
    elif are_calls == 0:
        options_df['is_call'] = 0
    options_df['stock_bid'] = quote['bid']
    options_df['stock_ask'] = quote['ask']
    options_df['created_at'] = int(time.time())
    return options_df


def download_yf_options(tickers, est_approx, output_path=OUTPUT_PATH):
    now = datetime.datetime.now()
    now_date_str = str(now.date())
    year = now.year
    file_dir = "{}/yf/options/{}/{}/".format(output_path, year, now_date_str)

    for ticker in tickers:
        # save a general ticker options request
        data = get_new_json("{}/v7/finance/options/{}".format(YAHOO_BASE_URL, ticker))
        if "error_code" not in data:
            general_filename = "{}-{}T{}.json".format(ticker, now_date_str, est_approx)
            ticker_df = pd.DataFrame()
        else:
            general_filename = "ERRORS/{}-{}T{}.json".format(ticker, now_date_str, est_approx)
        save_json(file_dir + general_filename, data)

        # save each options request by option expiration date
        if data['optionChain']['result']:
            for expiration_date in data['optionChain']['result'][0]['expirationDates']:
                opt_data = get_new_json("{}/v7/finance/options/{}?date={}".format(YAHOO_BASE_URL, ticker, expiration_date))
                if "error_code" not in opt_data:
                    specific_filename = "{}-{}T{}exp{}.json".format(ticker, now_date_str, est_approx, expiration_date)
                    # save a csv
                    if data['optionChain']['result']:
                        options = data['optionChain']['result'][0]['options'][0]
                        quote = data['optionChain']['result'][0]['quote']
                        calls_df = convert_options_json_to_df(options['calls'], 1, quote)
                        puts_df = convert_options_json_to_df(options['puts'], 0, quote)
                        ticker_df = ticker_df.append(calls_df, ignore_index=True)
                        ticker_df = ticker_df.append(puts_df, ignore_index=True)
                else:
                    specific_filename = "ERRORS/{}-{}T{}exp{}.json".format(ticker, now_date_str, est_approx, expiration_date)
                save_json(file_dir + specific_filename, opt_data)
        ticker_df.to_csv(file_dir + general_filename.replace(".json",".csv"), index=False)
