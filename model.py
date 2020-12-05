from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData, func
from sqlalchemy.orm import Session
import warnings
from sqlalchemy import exc as sa_exc
eng = create_engine('postgresql://stockarchivedb@localhost:5432/stockarchivedb')
conn = eng.connect()
conn.execute("""SET TIME ZONE 'UTC';""")

metadata = MetaData()
with warnings.catch_warnings():
    # suppress sqlalchemy warrnings from printing to console about unable to reflect database indexes
    warnings.simplefilter("ignore", category=sa_exc.SAWarning)
    metadata.reflect(eng, only=["yf_stock_daily", "yf_stock_intraday", "yf_option", "fv_stock_info", "fv_stock_daily", "ticker_group"])
Base = automap_base(metadata=metadata)
Base.prepare()

YfStockDaily = Base.classes.yf_stock_daily
YfStockIntraday = Base.classes.yf_stock_intraday
YfOption = Base.classes.yf_option
FvStockInfo = Base.classes.fv_stock_info
FvStockDaily = Base.classes.fv_stock_daily
TickerGroup = Base.classes.ticker_group

session = Session(eng)
