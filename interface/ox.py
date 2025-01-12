"""
    Interface for ox.fun 
    
    Docs: https://docs.ox.fun/#change-log
"""

import requests
import pandas as pd
from typing import List, Dict, Optional
import logging
import time
from datetime import datetime, timedelta

class OxDataCollector: 
    def __init__(self, api_key: str = None ):
        """
        Initialize the Ox data collector
        
        :param api_key: Your Ox API key
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.base_url = "https://api.ox.fun/v3/"

        self.rate_limit_window = 1
        self.max_requests = 100
        self.request_timestamps = []

    def check_rate_limit(self):
        """
        Ensure we don't exceed:
            - 100 requests per second
            - 2500 requests per 5mins 
        """
        now = time.time()
        five_minutes_ago = now - 300

        # Remove timestamps older than 5 minutes
        self.request_timestamps = [timestamp for timestamp in self.request_timestamps if timestamp > five_minutes_ago]

        # Check if we exceed 2500 requests in the last 5 minutes
        if len(self.request_timestamps) >= 2500:
            sleep_time = self.request_timestamps[0] + 300 - now
            self.logger.info(f"Rate limit reached (2500 requests/5 minutes), sleeping for {sleep_time:.2f} seconds")
            time.sleep(max(0, sleep_time))
            now = time.time()
        
        while len(self.request_timestamps) >= self.max_requests:
            window_start = now - self.rate_limit_window
            if self.request_timestamps[0] < window_start:
                self.request_timestamps.pop(0)
            else:
                sleep_time = self.request_timestamps[0] + self.rate_limit_window - now
                self.logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(max(0, sleep_time))
                now = time.time()
        self.request_timestamps.append(now)
    
    def getMarkets(self):
        """
            Fetch list of markets on OX
        """
        url = f"{self.base_url}markets"

        self.check_rate_limit()
        response = requests.get(url)
        response.raise_for_status()
        # return response.json()
        return pd.DataFrame(response.json().get('data', []))

    def getTickers(self):
        """
            Fetch list of tickers on OX
        """
        url = f"{self.base_url}tickers"

        self.check_rate_limit()
        response = requests.get(url)
        response.raise_for_status()
        # return response.json()
        return pd.DataFrame(response.json().get('data', []))
    
    def get_ohlcv(self, marketCode, timeframe, limit, start_time=None, end_time=None):
        """
            Fetch OHLCV data for a given market and timeframe

            :param marketCode: Market code
            :param timeframe: 60s,300s,900s,1800s,3600s,7200s,14400s,86400s, default is 3600s
            :param limit: Default 200, max 500
        """
        if end_time is None:
            # current utc time 
            end_time = int(datetime.now().timestamp()) * 1000
            start_time = int((datetime.now() - timedelta(days=7)).timestamp()) * 1000
        
        url = f"{self.base_url}candles?marketCode={marketCode}&timeframe={timeframe}&limit={limit}&startTime={start_time}&endTime={end_time}"
        logging.info(f"API call: {url}")

        self.check_rate_limit()
        response = requests.get(url)
        response.raise_for_status()
        ohlcv = pd.DataFrame(response.json().get('data', []))
        for col in ohlcv.columns:
            if col not in ['timeframe']:
                ohlcv[col] = pd.to_numeric(ohlcv[col])

        ohlcv['marketCode'] = marketCode

        # sort ascending by openedAt    
        ohlcv = ohlcv.sort_values('openedAt', ascending=True)

        return ohlcv
    
    def get_ohlcv_for_list_of_marketCodes(
            self, marketCodes: List[str], timeframe: str = '3600s', limit: int = 500, days_history: int = 20
    ) -> pd.DataFrame:
        """
            Fetch OHLCV data for a list of market codes

            :param marketCodes: List of market codes
            :param timeframe: 60s,300s,900s,1800s,3600s,7200s,14400s,86400s, default is 3600s
            :param limit: Default 200, max 500; num candles per request
        """
        logging.info(f"Fetching OHLCV data for {len(marketCodes)} markets")
        ohlcv_list = [] 
        for marketCode in marketCodes:
            symbol_data = [] 
            end_time = int(datetime.now().timestamp()) * 1000
            # set start time to api max of 7 days ago
            start_time = int((datetime.now() - timedelta(days=7)).timestamp()) * 1000
            while True:
                self.check_rate_limit()
                batch = self.get_ohlcv(marketCode, timeframe, limit, start_time, end_time)
                
                if isinstance(batch, list) and not batch:
                    self.logger.info(f"No more data for {marketCode}")
                    break
                if isinstance(batch, pd.DataFrame) and batch.empty:
                    self.logger.info(f"No more data for {marketCode}")
                    break
                symbol_data.append(batch)
                # if there are less than 7 days of data, we have reached the end
                if len(batch) < 7*24:
                    self.logger.info(f"No more data for {marketCode}")
                    break
                end_time = batch['openedAt'].min()
                start_time = int((datetime.fromtimestamp(end_time / 1000) - timedelta(days=7)).timestamp()) * 1000
               
                if end_time < (datetime.now().timestamp() - days_history * 24 * 60 * 60) * 1000:
                    break

                time.sleep(1) 
            if symbol_data:
                symbol_data = pd.concat(symbol_data, ignore_index=True)
                symbol_data = symbol_data.sort_values('openedAt', ascending=True)
                ohlcv_list.append(symbol_data)
            
            # convert min and max openedAt to human readable date
            min_date = datetime.fromtimestamp(ohlcv_list[-1]['openedAt'].min() / 1000)
            max_date = datetime.fromtimestamp(ohlcv_list[-1]['openedAt'].max() / 1000)

            logging.info(f"Fetched OHLCV data for {marketCode} from {min_date} to {max_date}")
            print(f"Fetched OHLCV data for {marketCode} from {min_date} to {max_date}")
            
            # ohlcv_list.append(self.get_ohlcv(marketCode, timeframe, limit))
        return pd.concat(ohlcv_list, ignore_index=True) if ohlcv_list else pd.DataFrame()