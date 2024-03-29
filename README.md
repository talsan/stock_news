# Collecting Stock News History using Diffbot
Leveraging [Diffbot's powerful Knowledge Graph](https://www.diffbot.com) to collect news from a pre-defined set of sources (e.g. wsj.com)
and a pre-defined set of companies (Russell 1000).

![Diffbot Example](https://github.com/talsan/stock_news/blob/master/assets/diffbot_example.PNG)

## Process Details
1. `build_stock_universe.py` -- Build a Mapped Universe
    1. Get R1000 Tickers from iShares holdings
    2. Get diffbot-entity-id by submitting `Ticker` to **Diffbot Knowledge Graph**
    3. Output goes to `./data/id_map.csv`
2. `sync_news.py` -- Get Articles by querying **Diffbot Knowledge Graph**
    1. query by diffbot-entity-id + news source + year
    2. Output goes to S3 (e.g. `diffbot-stock-news/type=kg_raw/version=202110.0/entity=E-6s5hEvCNFCnAQ2hpmLT8g/source=cnn.com/year=2019.gz`)
3. `build_corpus.py` - Collect all the data into a single object
    1. remove duplicates
    2. clean up text
    3. Output goes to `./data/news_extracts/R2000_201901_to_2021109_allnews.json` (everything in one place) and `./data/news_extracts/text_chunks/` (data is chunked for more efficient downstream multiprocessing/streaming)
 
## News Sources
```python
news_sources = ['bloomberg.com', 'wsj.com', 'reuters.com', 'barrons.com', 'nytimes.com', 'cnbc.com',
'marketwatch.com', 'ft.com', 'finance.yahoo.com', 'apnews.com', 'cnn.com',
'foxnews.com', 'foxbusiness.com']
```
