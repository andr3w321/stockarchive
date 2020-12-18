import finvizlite as fl
import yfinance as yf
import datetime
import requests
import time
import pandas as pd
import json
from sqlalchemy.sql import text
from dateutil.parser import parse
import pytz
from model import YfStockDaily, YfStockIntraday, YfOption, FvStockInfo, FvStockDaily, eng, conn, session, TickerGroup
import numpy as np
from numpy import inf

# NOTE could use black_scholes_merton model to take into account dividends for options pricing
# analytical functions compute exact greeks, numerical computes approximate greeks possibly faster
# source https://github.com/vollib/py_vollib/blob/master/py_vollib/black_scholes/greeks/numerical.py
from py_vollib.black_scholes.implied_volatility import implied_volatility
# from py_vollib.helpers.numerical_greeks import delta as numerical_delta
# from py_vollib.helpers.numerical_greeks import vega as numerical_vega
# from py_vollib.helpers.numerical_greeks import theta as numerical_theta
# from py_vollib.helpers.numerical_greeks import rho as numerical_rho
# from py_vollib.helpers.numerical_greeks import gamma as numerical_gamma
from py_vollib.black_scholes.greeks.analytical import gamma as agamma
from py_vollib.black_scholes.greeks.analytical import delta as adelta
from py_vollib.black_scholes.greeks.analytical import vega as avega
from py_vollib.black_scholes.greeks.analytical import rho as arho
from py_vollib.black_scholes.greeks.analytical import theta as atheta
from py_lets_be_rational.exceptions import BelowIntrinsicException
from py_lets_be_rational.exceptions import AboveMaximumException

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
    return ['ES=F','NQ=F','RTY=F','ZN=F','ZB=F','GC=F','SI=F','HG=F','CL=F','NG=F','ZC=F','ZO=F','KE=F','ZS=F','HE=F','LE=F','LBS=F']

def get_risk_free_rate_ticker():
    # 13 week US t-bill rate
    return "^IRX"

def get_sp500_tickers():
    """ Get all the sp500 tickers in a list and return it """
    # TODO add day parameter?
    tickers_tup = conn.execute(text("""SELECT tickers FROM ticker_group WHERE start_day = (SELECT start_day FROM ticker_group WHERE tag = 'sp500' ORDER BY start_day DESC LIMIT 1) AND tag = 'sp500';""")).fetchone()
    if len(tickers_tup) > 0:
        return tickers_tup[0]
    else:
        return []

def get_top_tickers(n, skip=0):
    """ Get the top n tickers by marketcap """
    # TODO add day parameter?
    tickers_tup = conn.execute(text("""SELECT ticker FROM fv_stock_daily WHERE day = (SELECT day FROM fv_stock_daily ORDER BY day DESC LIMIT 1) ORDER BY marketcap DESC NULLS LAST LIMIT :limit OFFSET :offset;"""), {"limit": n, "offset": skip}).fetchall()
    return tuple_to_list(tickers_tup)

### general functions
def was_market_open(day):
    """ Get the current SPY trading data
    if SPY trading data is for today then the market is open
    else market must be closed today """

    ticker = "SPY"
    period1 = int((datetime.datetime.now() - datetime.timedelta(days=30)).timestamp()) # 30 days ago
    period2 = int(time.time()) # now
    interval = "1d"
    url = "{}/v8/finance/chart/{}?symbol={}&period1={}&period2={}&interval={}".format(YAHOO_BASE_URL, ticker, ticker, period1, period2, interval)
    data = requests.get(url)
    json = data.json()
    meta = json['chart']['result'][0]['meta']
    gmtoffset = meta['gmtoffset']
    for dt in json['chart']['result'][0]['timestamp']:
        local_dt = datetime.datetime.utcfromtimestamp(dt) # maybe have to add gmtoffset?
        local_date = local_dt.date()
        if str(local_date) == day:
            return True
    else:
        return False

def was_market_open_today():
    return was_market_open(get_today_string())

def get_today_string():
    return str(datetime.date.today())

def get_new_json(url, headers={}):
    """ Request a url and return the json data, or return json data with an error code """
    print(url)
    res = requests.get(url, headers)
    if res.status_code == 200:
        return res.json()
    else:
        print("ERROR:", res.status_code, "URL:", url)
        return {"error_code": res.status_code, "error_msg": "URL Error", "url": url}

def iv_with_exception_handling(price, S, K, t, r, flag):
    if t < 0:
        return np.nan
    else:
        try:
           return implied_volatility(price, S, K, t, r, flag)
        except BelowIntrinsicException:
            return np.nan
        except AboveMaximumException:
            return np.nan

def delta_with_exception_handling(flag, S, K, t, r, sigma):
    if t < 0:
        return np.nan
    else:
        return adelta(flag, S, K, t, r, sigma)

def gamma_with_exception_handling(flag, S, K, t, r, sigma):
    if t < 0:
        return np.nan
    else:
        return agamma(flag, S, K, t, r, sigma)

def theta_with_exception_handling(flag, S, K, t, r, sigma):
    if t < 0:
        return np.nan
    else:
        return atheta(flag, S, K, t, r, sigma)

def vega_with_exception_handling(flag, S, K, t, r, sigma):
    if t < 0:
        return np.nan
    else:
        return avega(flag, S, K, t, r, sigma)

def tuple_decimals_to_list_floats(tup):
    return [float(x) for (x,) in tup]

def tuple_to_list(tup):
    return [x for (x,) in tup]

def get_sigma(ticker):
    """ Calculate the annualized volatility for a ticker"""
    db_t = session.query(YfStockDaily.close).filter(YfStockDaily.ticker == ticker).order_by(YfStockDaily.day.desc()).limit(252).all()
    # TODO if db_t rowcount < 1, raiseError
    ls_t = tuple_decimals_to_list_floats(db_t)
    closes = pd.DataFrame(ls_t, columns=['Close']) # need to convert to DataFrame so can perform shift(1)
    logreturns = np.log(closes / closes.shift(1))
    return np.sqrt(252*logreturns.var()).Close

def calc_hist_vol(df_closes, hv_lookback, vol_days=365):
    """ Calculate and return a rolling historical volatility column """
    # some formulas use 252 for vol_days
    log_return = np.log(df_closes / df_closes.shift(1))
    return log_return.rolling(window=hv_lookback).std() * np.sqrt(vol_days)

### parsing functions
def convert_marketcap_to_millions(text_marketcap):
    if 'B' in text_marketcap:
        text_marketcap = text_marketcap.replace('B','')
        marketcap = float(text_marketcap) * 1000.0
    if 'M' in text_marketcap:
        text_marketcap = text_marketcap.replace('M','')
        marketcap = float(text_marketcap)
    if '-' == text_marketcap:
        marketcap = None
    return marketcap

def convert_dash_to_none(text):
    if text == "-":
        return None
    else:
        return text

def convert_is_call_to_text(is_call):
    if is_call == 1:
        return 'c'
    else:
        return 'p'

def get_risk_free_rate(date_str=get_today_string()):
    db_rf = session.query(YfStockDaily).filter(YfStockDaily.ticker == get_risk_free_rate_ticker(), YfStockDaily.day <= date_str).order_by(YfStockDaily.day.desc()).first()
    if db_rf is None:
        # by default use 0 as the risk free rate
        return 0
    else:
        return max(0, float(db_rf.close)) / 100.0

def get_dte(exp_str, utc_now=parse(str(datetime.datetime.utcnow())+"+00")):
    """ Given an expiration date, calculate the days to expiration from now. Return as a days float. """
    exp_dt = parse(exp_str) + datetime.timedelta(days=1) # weird bug, have to add a day to expiration dt
    return (exp_dt - utc_now).total_seconds() / (24 * 60 * 60)

def calc_atm_column(itm):
    """ Given an itm column, add calculate an atm column for the two near the money options and return the list """
    atm = []
    for i in range(0,len(itm)):
        if len(itm) == 1:
            # if length is only 1, don't bother trying to calc atm
            atm.append(0)
        elif i == 0:
            if itm[i] != itm[i+1]:
                atm.append(1)
            else:
                atm.append(0)
        elif i == len(itm) - 1:
            if itm[i] != itm[i-1]:
                atm.append(1)
            else:
                atm.append(0)
        elif itm[i] != itm[i+1] or itm[i] != itm[i-1]:
            atm.append(1)
        else:
            atm.append(0)
    return atm

def get_unix_time(dt_str):
    """ Useful for converting timestamp with timezone back to option expiration date unix time that yahoo API uses """
    return int(parse(dt_str).timestamp())

def get_expiration_date_from_datetime(dt_str):
    """ Given '2020-11-05 16:00:00-08' returns 2020-11-6 """
    return parse(dt_str).astimezone(pytz.utc).date()

### import functions
def import_all_finviz_tickers():
    """ Import all 7000+ finviz tickers from the Overview page screener into the database.
    Create or update the fv_stock_info table.
    Create new fv_stock_daily rows. """
    df = fl.scrape_all("https://finviz.com/screener.ashx?v=111&o=-marketcap", print_urls=False)
    for row in df.values:
        n,ticker,company,sector,industry,country,marketcap,pe,price,change,volume = row
        ticker = ticker.upper()
        # for an update or create, need to find or create the new object before updating
        db_fi = session.query(FvStockInfo).filter_by(ticker = ticker).first()
        if db_fi is None:
            db_fi = FvStockInfo()
            create_new = True
        else:
            create_new = False
        db_fi.ticker = ticker
        db_fi.company = company
        db_fi.sector = sector
        db_fi.industry = industry
        db_fi.country = country 

        if create_new:
            session.add(db_fi)

        today = datetime.date.today()
        fd = FvStockDaily()
        fd.ticker = ticker
        fd.day = today
        fd.n = n
        fd.marketcap = convert_marketcap_to_millions(marketcap)
        fd.pe = convert_dash_to_none(pe)
        fd.price = price
        fd.change = change.replace('%','')
        fd.volume = volume.replace(',','')

        db_fd = session.query(FvStockDaily).filter_by(ticker = ticker, day = today).first()
        if db_fd is None:
            session.add(fd)

def import_sp500_finviz_tickers():
    """ Scrape all sp500 tickers from finviz and create a new ticker_group row in db if it's different than last time.
    Save the tickers by marketcap. Compare them by sorting alphabetically. """
    df = fl.scrape_all("https://finviz.com/screener.ashx?v=111&f=idx_sp500&o=-marketcap", print_urls=False)
    sp500_tickers_by_marketcap = df['Ticker'].tolist()
    sorted_sp500_tickers = sorted(sp500_tickers_by_marketcap)
    db_tg = session.query(TickerGroup).filter_by(tag = "sp500").order_by(TickerGroup.created_at.desc()).first()
    if db_tg is None or sorted(db_tg.tickers) != sorted_sp500_tickers:
        tg = TickerGroup()
        tg.start_day = datetime.date.today()
        tg.tag = "sp500"
        tg.tickers = sp500_tickers_by_marketcap
        session.add(tg)

def import_yf_daily_history(ticker, period):
    ticker = ticker.upper()
    df = yf.Ticker(ticker).history(period=period, interval="1d", prepost=False, actions=True, auto_adjust=True, back_adjust=False)
    df['ticker'] = ticker
    df.rename(columns = {'Open':'open','High':'high','Low':'low','Close':'close','Volume':'volume','Dividends':'dividends','Stock Splits':'stock_splits'}, inplace = True) 
    df.index.names = ['day']
    df['open'] = df['open'].round(4)
    df['high'] = df['high'].round(4)
    df['low'] = df['low'].round(4)
    df['close'] = df['close'].round(4)
    df['dividends'] = df['dividends'].round(4)
    df['stock_splits'] = df['stock_splits'].round(4)
    # some stock splits erroneously 1 for 0 (infinity), assume they are bugs, and replace them with 0s
    df["stock_splits"] = df["stock_splits"].replace(inf, 0)

    # delete all
    conn.execute(text("""DELETE FROM yf_stock_daily WHERE ticker = :ticker AND day >= :day """), {"ticker": ticker, "day": df.index[0]})
    # insert all
    df.to_sql('yf_stock_daily', con=eng, if_exists='append')

def import_yf_one_min_bars(ticker):
    ticker = ticker.upper()
    df = yf.Ticker(ticker).history(period="5d", interval="1m", prepost=True, actions=True, auto_adjust=True, back_adjust=False)
    df = df.reset_index() # change Datetime index to a column
    df['ticker'] = ticker
    df.rename(columns = {
        'Datetime':'bar_start',
        'Open':'open',
        'High':'high',
        'Low':'low',
        'Close':'close',
        'Volume':'volume',
        'Dividends':'dividends',
        'Stock Splits':'stock_splits'}, inplace = True) 

    df['open'] = df['open'].round(4)
    df['high'] = df['high'].round(4)
    df['low'] = df['low'].round(4)
    df['close'] = df['close'].round(4)
    df['dividends'] = df['dividends'].round(4)
    df['stock_splits'] = df['stock_splits'].round(4)
    # some stock splits erroneously 1 for 0 (infinity), assume they are bugs, and replace them with 0s
    df["stock_splits"] = df["stock_splits"].replace(inf, 0)

    # assume its always EST timezone
    if df["bar_start"][0].tzname() != "EST":
        raise ValueError("Non EST timezone parsing yf_one_min_bars for", ticker)
    # rth set
    df['rth'] = pd.to_datetime(df['bar_start']).dt.time.between(datetime.time(9,30),datetime.time(15,59)).astype(int)

    # delete all
    conn.execute(text("""DELETE FROM yf_stock_intraday WHERE ticker = :ticker AND bar_start >= :bar_start"""), {"ticker": ticker, "bar_start": df["bar_start"][0]})
    # insert all
    df.to_sql('yf_stock_intraday', con=eng, if_exists='append', index=False)

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
    options_df['underlying_bid'] = quote['bid']
    options_df['underlying_ask'] = quote['ask']
    return options_df

def get_options_df_and_exp_date(data):
    """ Given json data, return an options dataframe and the found expiration date for the data """
    ticker_df = pd.DataFrame()
    exp_date = None
    if "error_code" not in data:
        if data['optionChain']['result']:
            options = data['optionChain']['result'][0]['options'][0]
            exp_date = options["expirationDate"]
            quote = data['optionChain']['result'][0]['quote']
            calls_df = convert_options_json_to_df(options['calls'], 1, quote)
            puts_df = convert_options_json_to_df(options['puts'], 0, quote)
            ticker_df = ticker_df.append(calls_df, ignore_index=True)
            ticker_df = ticker_df.append(puts_df, ignore_index=True)
    return ticker_df, exp_date

def add_greeks(df, risk_free_rate=0, utc_now=parse(str(datetime.datetime.utcnow())+"+00")):
    df["price"] = (df["ask"] + df["bid"]) / 2.0
    #df["price"] = df["last_price"]
    df["S"] = (df["underlying_ask"] + df["underlying_bid"]) / 2.0
    df["dte"] = df.apply(lambda x: get_dte(x.expiration, utc_now), axis=1)
    df["yte"] = df["dte"] / 365.0
    df["flag"] = df.apply(lambda x: convert_is_call_to_text(x.is_call), axis=1)
    # sigmas
    sigmas = {}
    unq_tickers = df["ticker"].unique()
    for ticker in unq_tickers:
        sigmas[ticker] = get_sigma(ticker)

    df["implied_vol"] = df.apply(lambda x: iv_with_exception_handling(x.price, x.S, x.strike, x.yte, risk_free_rate, x.flag), axis=1)
    df["delta"] = df.apply(lambda x: delta_with_exception_handling(x.flag, x.S, x.strike, x.yte, risk_free_rate, sigmas[x.ticker]), axis=1)
    df["gamma"] = df.apply(lambda x: gamma_with_exception_handling(x.flag, x.S, x.strike, x.yte, risk_free_rate, sigmas[x.ticker]), axis=1)
    df["theta"] = df.apply(lambda x: theta_with_exception_handling(x.flag, x.S, x.strike, x.yte, risk_free_rate, sigmas[x.ticker]), axis=1)
    df["vega"] = df.apply(lambda x: vega_with_exception_handling(x.flag, x.S, x.strike, x.yte, risk_free_rate, sigmas[x.ticker]), axis=1)

    return df

def import_yf_options(ticker, scrape_time, risk_free_rate=0):
    today = datetime.date.today()
    data = get_new_json("{}/v7/finance/options/{}".format(YAHOO_BASE_URL, ticker)) # no date appended, returns closest exp date option chain
    req_count = 1
    ticker_df, general_exp_date = get_options_df_and_exp_date(data)
    if "error_code" not in data:
        # save each options request by option expiration date
        if data['optionChain']['result']:
            for expiration_date in data['optionChain']['result'][0]['expirationDates']:
                if expiration_date != general_exp_date: # don't send the first date request twice
                    opt_data = get_new_json("{}/v7/finance/options/{}?date={}".format(YAHOO_BASE_URL, ticker, expiration_date))
                    req_count += 1
                    chain_df, chain_exp_date = get_options_df_and_exp_date(opt_data)
                    ticker_df = ticker_df.append(chain_df)

        if ticker_df["currency"].values[0] != "USD":
            print("Irregular currency for", ticker, today, scrape_time)
        if ticker_df["contractSize"].values[0] != "REGULAR":
            print("Irregular contractSize for", ticker, today, scrape_time)

        # not saving this yahoo data
        del ticker_df["change"]
        del ticker_df["percentChange"]
        del ticker_df["currency"]
        del ticker_df["contractSize"]

        # rename columns
        ticker_df.rename(columns = {
            'contractSymbol':'contract_symbol',
            'lastTradeDate':'last_trade_date',
            'lastPrice':'last_price',
            'openInterest':'open_interest',
            'impliedVolatility':'yf_implied_vol',
            'inTheMoney':'itm'}, inplace = True) 

        # add and modify a few columns
        ticker_df["ticker"] = ticker
        ticker_df["scrape_day"] = today
        ticker_df["scrape_time"] = scrape_time
        ticker_df["atm"] = calc_atm_column(ticker_df["itm"].tolist())
        ticker_df["itm"] = ticker_df["itm"].astype(int)
        ticker_df["last_trade_date"] = pd.to_datetime(ticker_df["last_trade_date"], unit='s', utc=True)
        ticker_df["expiration"] = pd.to_datetime(ticker_df["expiration"], unit='s', utc=True)

        # greeks
        ticker_df = add_greeks(ticker_df, risk_free_rate)
            
        #ticker_df.drop_duplicates(inplace=True) # there shouldnt be any duplicate rows
        # sometimes during development we'll need to delete rows, but usually this line should be commented out
        #conn.execute(text("""DELETE FROM yf_option WHERE ticker = :ticker AND scrape_day = :scrape_day AND scrape_time = :scrape_time"""), {"ticker": ticker, "scrape_day": today, "scrape_time": scrape_time})
        ticker_df.to_sql('yf_option', con=eng, if_exists='append', index=False)
        return req_count
