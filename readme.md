# splitflow
> super parallel webcrawler + stock split arbitrage trading bot

## what is this?
- scrapes the sec database of 10k and 8k filings 
- if it detects upcoming reverse stock splits, it will watch the stock
- a day before the split, it will buy the stock and hold it
- once the split is complete, it will sell the stock!

TODO: dockerize so i don't need to pay mongodb

## why?
- i am broke and the quant firms aren't giving me a job
- i need a production-style rust project to show off that isn't closed source. 
- This one i built in 6+3 days while also learning diffeq in the span of a week

## features
- very fast scanning!
- discord interface so i can watch from my phone!
- fully persistent and designed with cpu parallelism and io concurrency in mind
- automated!
- ai accelerated™
- **please delete the commit history and the api keys before you publish this**
  - i dont care aymore

## technical description
- Scans in parallel!
- Uses local ml inference to detect stock split details! (not rn lol)
- Mongodb with a cache in front for storage!
- postgres for task tracking!
- robinhood support, will add more stuff later!

## next step
- rewrite to support many users and sell an easier version of it on whop!
- automated user onboarding and whop api integration
- if the whop users find this and are skilled enough to compile it they can use it!
