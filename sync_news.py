import os
import logging
import multiprocessing as mp
from datetime import datetime
from utils import diffbot_api, s3_helpers
import itertools
import config
from multiprocessing_logging import install_mp_handler

log = logging.getLogger(__name__)

def get_existing_downloads():
    path_prefix = f'type=kg_raw/version=202110.0/'
    existing_downloads = s3_helpers.list_keys(Bucket=config.Aws.S3_NEWS_BUCKET, Prefix=path_prefix, full_path=False,
                                              remove_ext=True)
    existing_downloads = [{'entity': y[0].replace('entity=', ''),
                           'source': y[1].replace('source=', ''),
                           'year': y[2].replace('year=', '')}
                          for y in [x.split('/') for x in existing_downloads]]
    return existing_downloads


def build_download_queue(overwrite=False):
    r1000_wts = s3_helpers.get_etf_holdings('IWB', '2021-09-30')
    r1000_mapped = diffbot_api.load_org_info()
    entities = r1000_mapped.merge(r1000_wts, how='inner', on='ticker') \
        .sort_values(by='weight', ascending=False).dropna(subset=['id'])['id'].to_list()

    # sources = pd.read_csv('./data/credible_sources/credible_sources.csv')['website'].to_list()
    sources = ['bloomberg.com', 'wsj.com', 'reuters.com', 'barrons.com', 'nytimes.com', 'cnbc.com',
               'marketwatch.com', 'ft.com', 'finance.yahoo.com', 'apnews.com', 'cnn.com',
               'foxnews.com', 'foxbusiness.com']
    years = [str(y) for y in range(2019, 2022)]

    download_queue = [{'entity': x[0], 'source': x[1], 'year': x[2]}
                      for x in list(itertools.product(entities, sources, years))]
    if not overwrite:
        existing_downloads = get_existing_downloads()
        download_queue = [q for q in download_queue if q not in existing_downloads]

    return download_queue


def sync_news(request_obj):
    diffbot_api.search_entity_news_and_save(**request_obj)

if __name__ == "__main__":
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    install_mp_handler()

    download_queue = build_download_queue(overwrite=False)

    for d in download_queue:
        diffbot_api.search_entity_news_and_save(**d)

    #pool = mp.Pool(processes=mp.cpu_count())
    #pool = mp.Pool(processes=4)
    #pool.map(sync_news, download_queue)
