from utils import s3_helpers, iex_api, diffbot_api
from utils import utils
import pandas as pd
import logging
import config
import requests

logging.basicConfig(level=logging.INFO)

# get list of index constituents at year ends
r1000_list = [s3_helpers.get_etf_holdings('IWB', biz_eomonth)[['asofdate', 'ticker', 'name', 'sector']]
              for biz_eomonth in ['2017-12-29', '2018-12-31', '2019-12-31', '2020-12-31', '2021-09-30']]

# combine them all into a unique list (~1300 names)
r1000 = pd.concat(r1000_list).dropna()
r1000_ticker_list = r1000['ticker'].str.upper().drop_duplicates().to_list()

# for each ticker, 1) request company info from iex api; 2) save response to local dir (defined in config.py)
iex_api.get_companyinfo_and_save(list_of_tickers=r1000_ticker_list,
                                 save_to_dir=config.Iex.IWS_COMPANY_OUTPUT_DIR,
                                 update_existing=False)
# load iex company info from local dir
r1000_info_iex = iex_api.load_company_info()

# for each ticker search kg
diffbot_api.search_org_and_save(list_of_tickers=r1000_ticker_list,
                                save_to_dir=config.Diffbot.KG_ORG_OUTPUT_DIR,
                                update_existing=False)
# load diffbot company info from local dir
r1000_info = diffbot_api.load_org_info()

# test = iex_api.get_company_info(ticker)

# source news for each one

# save results
