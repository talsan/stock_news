# Collecting Stock News History from the Web

### Master Plan
1. Get R1000 Tickers from iShares holdings
3. Get diffbot-entity-id by submitting [Name + Website] to `Diffbot Knowledge Graph` 
4. Get Articles by querying `Diffbot Knowledge Graph`, with filters:
    1. diffbot-entity-id
    2. time range (after 2017-12-21)
    3. credible news sources (sourced from spain consulting, llc)