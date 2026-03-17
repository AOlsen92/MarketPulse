# ingestion/api_client.py
import os, requests, time, logging
from dotenv import load_dotenv

load_dotenv()

class AlphaVantageClient:
    BASE_URL = 'https://www.alphavantage.co/query'
    RATE_LIMIT_SECONDS = 12  # 5 requests/min on free tier

    def __init__(self):
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.last_call = 0

    def get(self, function, symbol, **kwargs):
        elapsed = time.time() - self.last_call
        if elapsed < self.RATE_LIMIT_SECONDS:
            time.sleep(self.RATE_LIMIT_SECONDS - elapsed)
        params = {'function': function, 'symbol': symbol,
                  'apikey': self.api_key, **kwargs}
        resp = requests.get(self.BASE_URL, params=params)
        resp.raise_for_status()
        self.last_call = time.time()
        return resp.json()
