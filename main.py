# Standard Library
import itertools
import uvicorn
import threading
import asyncio
import pandas as pd
from pytz import timezone
import json
import requests
import logging
import sys

# Third Party
from fastapi import FastAPI
import snscrape.modules.twitter as sntwitter
from datetime import datetime
from config import *
import time

# init logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

app = FastAPI()

headers = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "x-api-key": X_API_KEY
}


@app.get("/health-check")
def health_check():
    return {"status": "healthy"}


def scrape_twitter_data():
    logger.info(f"thread started.")
    QUERY = f"""("1.kat" OR "2.kat" OR "3.kat" OR "4.kat" OR "5.kat" OR "6.kat" OR "7.kat" OR "8.kat" OR "9.kat" OR "10.kat" OR "11.kat") OR ("birincikat" OR "ikincikat" OR "üçüncükat" OR "dördüncükat" OR "beşincikat" OR "altıncıkat" OR "yedincikat" OR "sekizincikat" OR "dokuzuncukat" OR "onuncukat" OR "onbirincikat") OR ("bina" OR "apartman" OR "apt" OR "mahalle" OR "mahallesi" OR "bulvar" OR "sokak" OR "bulvarı" OR "göçük altında" OR "daire" OR "sk" OR "no:") lang:tr"""

    while True:
        timestamp = json.load(open('data.json', 'r'))['timestamp']
        turkey = timezone("Europe/Istanbul")

        # Veri Toplama
        logger.info(f"Data scraping process is started! timestamp:{timestamp}")
        df = pd.DataFrame(
            itertools.islice(sntwitter.TwitterSearchScraper(f"{QUERY} since_time:{timestamp}").get_items(), 1000))
        df["date"] = df.date.apply(lambda x: pd.to_datetime(str(pd.to_datetime(x).astimezone(turkey))[:-6]))
        logger.info(f"Data scraping process is done! count:{len(df)}")
        # Timestamp güncelleme
        json.dump({'timestamp': int(datetime.timestamp(df.date.max()))}, open('data.json', 'w'))

        # Backend için veri hazırlığı

        extra_parameters = []
        for index in df.index:
            m = {}
            m['user_id'] = df.loc[index]["user"]["id"]
            m['screen_name'] = df.loc[index]["user"]["username"]
            m['name'] = df.loc[index]["user"]["displayname"]
            m['tweet_id'] = df.loc[index]["id"]
            m['created_at'] = df.loc[index]["date"]
            m['hashtags'] = df.loc[index]["hashtags"]
            m['user_account_created_at'] = df.loc[index]["user"]["displayname"]
            m['media'] = df.loc[index]["media"]
            extra_parameters.append(m)
        df['extra_parameters'] = extra_parameters

        df['epoch'] = df.date.apply(lambda x: int(datetime.timestamp(x)))
        df['channel'] = 'twitter'
        df['extra_parameters'] = df['extra_parameters'].astype(str)
        df.rename(columns={'renderedContent': 'raw_data'}, inplace=True)
        df.drop_duplicates('raw_data', inplace=True)

        df = df[['raw_data', 'channel', 'extra_parameters', 'epoch']]

        logger.info(f"Requesting apigo process is started!")
        for row in df.iterrows():
            payload = {
                'raw_data': row[1]['raw_data'],
                'channel': row[1]['channel'],
                'extra_parameters': row[1]['extra_parameters'],
                'epoch': row[1]['epoch']
            }

            # response = requests.post('https://apigo.afetharita.com/events', payload=payload, headers=headers)
            # if response.status_code != 200:
            #    logger.error(f"Error in events with {response.status_code}")

        logger.info(f"Process done with {len(df)}")
        time.sleep(30)


if __name__ == '__main__':
    logger.info(f"{app_name} started.")
    twitter_scraper_thread = threading.Thread(target=scrape_twitter_data)
    twitter_scraper_thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
