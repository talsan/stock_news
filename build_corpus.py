import config
import json
from utils import s3_helpers
import re

# loop through s3 news objects
# only keep the good stuff
# format text
# structure and append to growing list

# remove/count duplicates

path_prefix = f'type=kg_raw/version=202110.0/'
existing_downloads = s3_helpers.list_keys(Bucket=config.Aws.S3_NEWS_BUCKET, Prefix=path_prefix, full_path=True,
                                          remove_ext=False)
all_news = []
for i, k in enumerate(existing_downloads[0:10]):
    print(round(i / len(existing_downloads), 4))
    entity_id = re.search('entity=([^/]*)/', k).group().replace('entity=', '').rstrip('/')
    source = re.search('source=([^/]*)/', k).group().replace('source=', '').rstrip('/')
    v = s3_helpers.get_json_from_s3(bucket=config.Aws.S3_NEWS_BUCKET, key=k)

    if v['results'] > 0:
        for d in v['data']:
            if all([k in d for k in ['tags', 'id', 'date']]):

                entity_tags = [t for t in d['tags'] if t['uri'].split('/')[-1][1:] in entity_id[1:]]

                if len(entity_tags) > 0:
                    entity_tag = entity_tags[0]
                else:
                    print(k)
                    print([t['uri'] for t in d['tags']])
                    entity_tag = {}

                tag_list = '|'.join([t['label'] for t in d['tags']])
                text_list = [txt.strip().replace('“', '"').replace('”', '"').replace('’', '\'')
                             for txt in d['text'].split('\n')
                             if (len(txt) > 50) and (txt.count(' ') > 3)]

                news_obj = {'entity_id': entity_id,
                            'source': source,
                            'date': d['date']['str'][1:11],
                            'article_id': d['id'],
                            'page_url': d.get('page_url'),
                            'tags': tag_list,
                            'entity_tag_score': entity_tag.get('score'),
                            'sentiment': d.get('sentiment'),
                            'title': d.get('title'),
                            'text': text_list}
                all_news.append(news_obj)

with open('./data/news_extracts/R2000_201901_to_202109_test.json', 'w', encoding='utf-8') as f:
    json.dump(all_news, f, indent=4, ensure_ascii=False)
