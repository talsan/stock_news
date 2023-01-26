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
for i, k in enumerate(existing_downloads):
    print(round(i / len(existing_downloads), 4))
    entity_id = re.search('entity=([^/]*)/', k).group().replace('entity=', '').rstrip('/')
    source = re.search('source=([^/]*)/', k).group().replace('source=', '').rstrip('/')
    v = s3_helpers.get_json_from_s3(bucket=config.Aws.S3_NEWS_BUCKET, key=k)

    if (v.get('results') is None) or (v['results'] > 0):
        if v.get('data') is not None:
            for d in v['data']:
                if all([k in d for k in ['tags', 'id', 'date']]):

                    entity_tags = [t for t in d['tags'] if t['uri'].split('/')[-1][1:] in entity_id[1:]]

                    if len(entity_tags) > 0:
                        entity_tag = entity_tags[0]
                    else:
                        print(k)
                        print([t['uri'] for t in d['tags']])
                        entity_tag = {}

                    for t in d['tags']:
                        if t.get('types'):
                            t['types'] = [ty.split('/')[-1].lower() for ty in t['types']]

                    text_list = [txt.strip().replace('“', '"').replace('”', '"').replace('’', '\'')
                                 for txt in d['text'].split('\n')
                                 if (len(txt) > 50) and (txt.count(' ') > 3)]

                    news_obj = {'entity_id': entity_id,
                                'source': source,
                                'date': d['date']['str'][1:11],
                                'article_id': d['id'],
                                'page_url': d.get('pageUrl'),
                                'author': d.get('author'),
                                'tags': d['tags'],
                                'sentiment': d.get('sentiment'),
                                'title': d.get('title'),
                                'text': text_list}
                    all_news.append(news_obj)




with open('./data/news_extracts/R1000_201601_to_202206_allnews.json', 'w', encoding='utf-8') as f:
    json.dump(all_news, f, indent=4, ensure_ascii=False)

# # save only unique articles
# with open('./data/news_extracts/R1000_201601_to_202205_allnews.json', 'r', encoding='utf-8') as f:
#     all_news = json.load(f)
#
# corpus = {}
# for i, article in enumerate(all_news):
#     print(round(i / len(all_news), 4))
#     if 'article' not in corpus.keys():
#         corpus.update({article['article_id']: article['text']})
#
# # output in chunks
# chunksize = 1000
# # corpus = list(corpus.values())
# corpus = [{'article_id': k, 'text': v} for k, v in corpus.items()]
# for pos in range(0, len(corpus), chunksize):
#     with open(f'./data/news_extracts/text_chunks/R1000_201601_to_202205_textchunk_{pos}.json', 'w',
#               encoding='utf-8') as f:
#         json.dump(corpus[pos: pos + chunksize], f, indent=4, ensure_ascii=False)
