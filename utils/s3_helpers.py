import boto3
from botocore.exceptions import ClientError
import config
import pandas as pd
from io import BytesIO
import re
import logging
import gzip
import shutil
import json

logger = logging.getLogger(__name__)

# GLOBAL DATA (AWS CLIENT)
aws_session = boto3.Session(aws_access_key_id=config.Aws.AWS_KEY,
                            aws_secret_access_key=config.Aws.AWS_SECRET)

s3_client = aws_session.client('s3', region_name=config.Aws.S3_REGION_NAME)


def list_keys(Bucket, Prefix='', Suffix='', full_path=True, remove_ext=False):
    # get pages for bucket and prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=Bucket, Prefix=Prefix)

    # iterate through pages and store the keys in a list
    keys = []
    for page in page_iterator:
        if 'Contents' in page.keys():
            for content in page['Contents']:
                key = content['Key']
                if not key.endswith('/'):  # ignore directories
                    if key.endswith(Suffix):
                        if not full_path:
                            key = re.sub(Prefix, '', key)
                        if remove_ext:
                            key = re.sub('\.[^.]+$', '', key)
                        keys.append(key)
    return keys


def get_etf_holdings(etf_ticker, asofdate):
    s3_key = f'type=holdings/state=formatted/etf={etf_ticker}/asofdate={asofdate}.csv'
    print(s3_key)

    try:
        obj = s3_client.get_object(Bucket=config.Aws.S3_ETF_HOLDINGS_BUCKET,
                                   Key=s3_key)
    except ClientError as e:
        logger.error(f'{e.response["Error"]["Code"]}\n'
                     f'cant find file {s3_key}')
        raise
    df = pd.read_csv(BytesIO(obj['Body'].read()), dtype=str)
    return df


def write_response_s3(bucket, key, input_json):
    input_file_buffer = BytesIO(input_json)
    compressed_file_buffer = BytesIO()

    with gzip.GzipFile(fileobj=compressed_file_buffer, mode='wb') as gz:
        shutil.copyfileobj(input_file_buffer, gz)
    compressed_file_buffer.seek(0)

    s3_client.upload_fileobj(Bucket=bucket,
                             Key=key,
                             Fileobj=compressed_file_buffer,
                             ExtraArgs={'ContentType': 'application/json',
                                        'ContentEncoding': 'gzip'})

    s3_output_url = f'{config.Aws.S3_OBJECT_ROOT}/{bucket}/{key}'
    logger.info(f'archive raw to s3 success: {s3_output_url}')


def get_json_from_s3(bucket, key):
    input_file_buffer = BytesIO()
    output_file_buffer = BytesIO()

    s3_client.download_fileobj(Bucket=bucket,
                               Key=key,
                               Fileobj=input_file_buffer)

    input_file_buffer.seek(0)
    with gzip.GzipFile(fileobj=input_file_buffer, mode='rb') as gz:
        shutil.copyfileobj(gz, output_file_buffer)

    return json.loads(output_file_buffer.getvalue())