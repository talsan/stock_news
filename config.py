import os
from dotenv import load_dotenv

load_dotenv()


class Aws:
    S3_REGION_NAME = 'us-west-2'
    S3_ETF_HOLDINGS_BUCKET = 'ishares'
    S3_NEWS_BUCKET = 'diffbot-stock-news'
    S3_OBJECT_ROOT = 'https://s3.console.aws.amazon.com/s3/object'

    ATHENA_REGION_NAME = 'us-west-2'
    ATHENA_WORKGROUP = 'qc'
    ATHENA_OUTPUT_BUCKET = 'ishares-athena-output'

    ATHENA_SLEEP_BETWEEN_REQUESTS = 3
    ATHENA_QUERY_TIMEOUT = 200

    AWS_KEY = os.environ.get('AWS_KEY')
    AWS_SECRET = os.environ.get('AWS_SECRET')


class Iex:
    IWS_ROOT = 'https://cloud.iexapis.com/stable'
    IWS_KEY = os.environ.get('IEX_TOKEN')
    IWS_COMPANY_OUTPUT_DIR = './data/iex_company'

class Diffbot:
    KG_ENDPOINT = 'https://kg.diffbot.com/kg/dql_endpoint'
    DIFFBOT_KEY = os.environ.get('DIFFBOT_KEY')
    KG_ORG_OUTPUT_DIR = './data/diffbot_kg_org'