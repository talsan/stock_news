import config
import logging
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import json
import os

logger = logging.getLogger(__name__)

s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[402, 429])
s.mount('http://', HTTPAdapter(max_retries=retries))


def log_response_text(resp, *args, **kwargs):
    logger.info(f'Got response {resp.text[0:100]}... from {resp.url}')


def get_company_info(ticker):
    try:
        r = s.get(f'{config.Iex.IWS_ROOT}/stock/{ticker}/company?',
                  params={'token': config.Iex.IWS_KEY},
                  hooks={'response': [log_response_text]})
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as errh:
        logger.error(f'Http Error:{errh}')
    except requests.exceptions.ConnectionError as errc:
        logger.error(f'Error Connecting:{errc}')
    except requests.exceptions.Timeout as errt:
        logger.error(f'Timeout Error:{errt}')
    except requests.exceptions.RequestException as err:
        logger.error(f'Requests Error:{err}')
    except BaseException as errb:
        logger.error(f'Base Error:{errb}')


def write_response_local(resp_json, output_dir, ticker):
    if resp_json is not None:
        try:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = os.path.join(output_dir, f'{ticker.upper()}__{timestamp}.json')
            with open(file_name, 'w') as f:
                json.dump(resp_json, f)
            logger.info(f'Wrote response to local dir:{file_name}')
        except BaseException as errb:
            logger.error(f'Base Error:{errb}')
    else:
        logger.warning(f'No info: did not write {ticker} to local dir')


def get_companyinfo_and_save(list_of_tickers, save_to_dir=None, update_existing=False):
    assert (isinstance(list_of_tickers, list))

    if save_to_dir is None:
        save_to_dir = config.Iex.IWS_COMPANY_OUTPUT_DIR

    if not update_existing:
        existing_tickers = [filename.split('__')[0] for filename in os.listdir(config.Diffbot.KG_ORG_OUTPUT_DIR)]
        list_of_tickers = list(set(list_of_tickers) - set(existing_tickers))

    for ticker in list_of_tickers:
        company_info = get_company_info(ticker)
        write_response_local(resp_json=company_info, output_dir=save_to_dir, ticker=ticker)


def load_company_info(company_info_dir=None):
    if company_info_dir is None:
        company_info_dir = config.Iex.IWS_COMPANY_OUTPUT_DIR

    company_info = []
    info_files = os.listdir(company_info_dir)
    for file_name in info_files:
        with open(os.path.join(company_info_dir, file_name)) as f:
            r_dict = json.load(f)
        r_dict['tags'] = ','.join(r_dict['tags'])
        company_info.append(r_dict)

    return pd.DataFrame(company_info)


def collapse_tags(r_dict):
    r_dict['tags'] = '|'.join(r_dict['tags'])
