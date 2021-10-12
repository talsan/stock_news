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
retries = Retry(total=5, backoff_factor=1, status_forcelist=[400, 422, 500])
s.mount('http://', HTTPAdapter(max_retries=retries))


def log_response_text(resp, *args, **kwargs):
    logger.info(f'Got response {resp.text[0:100]}... from {resp.url}')


def kg_search(ticker):
    input_params = {'query': f'type:Organization isPublic:true stock.symbol:"{ticker}"',
                    'token': config.Diffbot.DIFFBOT_KEY}

    try:
        r = s.get(url=f'{config.Diffbot.KG_ENDPOINT}',
                  params=input_params,
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


def write_response_local(resp_json, kg_org_output_dir, ticker):
    if resp_json is not None:
        try:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = os.path.join(kg_org_output_dir, f'{ticker.upper()}__{timestamp}.json')
            with open(file_name, 'w') as f:
                json.dump(resp_json, f, indent=4)
            logger.info(f'Wrote response to local dir:{file_name}')
        except BaseException as errb:
            logger.error(f'Base Error:{errb}')
    else:
        logger.warning(f'No info: did not write {ticker} to local dir')


def search_entityid_and_save(list_of_tickers, save_to_dir=None, update_existing=False):
    assert (isinstance(list_of_tickers, list))

    if save_to_dir is None:
        save_to_dir = config.Diffbot.KG_ORG_OUTPUT_DIR

    if not update_existing:
        existing_tickers = [filename.split('__')[0] for filename in os.listdir(config.Diffbot.KG_ORG_OUTPUT_DIR)]
        list_of_tickers = list(set(list_of_tickers) - set(existing_tickers))

    for ticker in list_of_tickers:
        company_info = kg_search(ticker)
        write_response_local(resp_json=company_info, kg_org_output_dir=save_to_dir, ticker=ticker)


def load_company_info(kg_org_output_dir=None):
    if kg_org_output_dir is None:
        kg_org_output_dir = config.Diffbot.KG_ORG_OUTPUT_DIR

    company_info = []
    info_files = os.listdir(kg_org_output_dir)
    for file_name in info_files:
        with open(os.path.join(kg_org_output_dir, file_name)) as f:
            r_json = json.load(f)

        r_dict = {'ticker': file_name.split('__')[0]}
        r_dict_info = unpack_json_response(r_json)
        if r_dict_info is not None:
            r_dict.update(r_dict_info)
        company_info.append(r_dict)

    return pd.DataFrame(company_info)


def unpack_json_response(r_json):
    if r_json['results'] > 0:
        top_result = r_json['data'][0]
        output = {'id': top_result['id'],
                  'name': top_result['name'],
                  'stock_symbol': top_result['stock']['symbol'],
                  'stock_exchange': top_result['stock']['exchange'],
                  'website': top_result.get('homepageUri'),
                  'type': top_result['type'],
                  'importance': top_result['importance'],
                  'public': top_result['isPublic']
                  }
        return output


# # run diffbot enhance
# for i, row in r1000_info.iterrows():
#     if row.get('companyName') is not None:
#         input_params = {'name': utils.clean_name(row['companyName']),
#                         'url': row['website'],
#                         'phone': row['phone'],
#                         'location': f'{row.get("address")} {row.get("city")}'}
#
# params = {'type': 'Organization',
#           'token': config.Diffbot.DIFFBOT_KEY,
#           'size': 10}
# params.update(input_params)
# test = requests.get(url=f'{config.Diffbot.DIFFBOT_ROOT}/kg/v3/enhance_endpoint',
#                     params=params)
#