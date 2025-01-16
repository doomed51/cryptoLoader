import requests 
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from interface.base import BaseExchangeInterface
import time 
pd.set_option('future.no_silent_downcasting', True)

REQUEST_MAX_PERIODS = 500 # Responses that take a time range will only return 500 elements or distinct blocks of data
RATE_LIMIT_PER_MINUTE = 60

class hyperliquidDataCollector(BaseExchangeInterface): 
    def __init__(self, api_key: str = None):
        self.base_url = 'https://api.hyperliquid.xyz/'
        self.request_timestamps = []
        pass
    
    def check_rate_limit(self):
        """
        Ensure we don't exceed:
            - 60 requests per minute
        """
        now = time.time()
        one_minute_ago = now - 60

        # Remove timestamps older than 1 minute
        self.request_timestamps = [timestamp for timestamp in self.request_timestamps if timestamp > one_minute_ago]

        while len(self.request_timestamps) >= RATE_LIMIT_PER_MINUTE:
            window_start = now - 60
            if self.request_timestamps[0] < window_start:
                self.request_timestamps.pop(0)
            else:
                sleep_time = self.request_timestamps[0] + 60 - now
                self.logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(max(0, sleep_time))
                now = time.time()
        self.request_timestamps.append(now)
        print(f"One min ago: {one_minute_ago}")
        print(f"Rate limit check: {len(self.request_timestamps)}")

    def get_tickers(self) -> pd.DataFrame:
        """Get current market tickers"""
        print('HL not implemented')
        pass

    def get_markets(self) -> pd.DataFrame:
        """Get available markets"""
        print('HL not implemented')
        pass

    def get_perps(self) -> pd.DataFrame:
        """Get available perpetual markets"""
        headers = {
            "Content-Type": "application/json"
        }
        params = {
            'type':'meta'
        }
        url = self.base_url + 'info'

        response = requests.post(url, headers=headers, json=params)

        if response.status_code == 200:
            data = response.json().get('universe', [])
            # perps = [d for d in data if d['type'] == 'perpetual']
            data = pd.DataFrame(data)

            data['isDelisted'] = data['isDelisted'].fillna(False)
            return pd.DataFrame(data)
        else:
            print(f"Error fetching perpetual markets: {response.status_code}")
            return []
        
    def get_ohlcv(
        self,
        symbol: str,
        interval: str = '1h',
        limit: int = 100,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        print('HL not implemented')
        """Get OHLCV candle data"""
        pass

    def get_ohlcv_for_symbols(
        self,
        symbols: List[str],
        interval: str = '1h', 
        limit: int = 100,
        days_history: int = 30
    ) -> pd.DataFrame:
        print('HL not implemented')
        """Get OHLCV data for multiple symbols"""
        pass

    def get_funding_history(self, symbol: str, startTime: int=None, endTime:int = None) -> pd.DataFrame:
        """
            Get funding history for a specific token
            :param symbol: Hyperliquid trading pair symbol
            :param startTime: Start time in milliseconds
            :param endTime: End time in milliseconds. Defaults to current time 

            :return: DataFrame with funding history

            Leave startTime and endTime as None to get all available history for the symbol
        """
        # add type, coin, startTime, endTime to request body 
        url = self.base_url + 'info'
        headers = {
            "Content-Type": "application/json"
        }
        
        # set start and end times 
        if startTime is None:
            startTime = int((datetime.now() - timedelta(hours=REQUEST_MAX_PERIODS)).timestamp()) * 1000
            endTime = int(datetime.now().timestamp()) * 1000
        elif endTime is None:
            endTime = int(datetime.now().timestamp()) * 1000
        
        hist = [] 
        while True:
            params = {
                'type': 'fundingHistory',
                'coin': symbol,
                'startTime': startTime,
                'endTime': endTime
            }
            self.check_rate_limit()
            response = requests.post(url, json=params, headers=headers)

            if response.status_code == 200:
                localHist = pd.DataFrame(response.json())
            else:
                print(f"Error fetching funding history data: {response.status_code}")
                return hist
            
            # if we have data add it to hist 
            if len(localHist) > 0:
                hist.append(localHist)
            else:
                print(f"No more data for {symbol}")
                break

            if len(localHist) < 500:
                print(f"No more data for {symbol}")
                break
            
            # update startTime and endTime 
            endTime = int(localHist['time'].min())
            startTime = int(endTime - (REQUEST_MAX_PERIODS * 60 * 60 * 1000))
            
        hist = pd.concat(hist).reset_index(drop=True)
        hist.columns = ['symbol','fundingRate','premium','time']
        # drop any time duplicates
        hist = hist.drop_duplicates(subset='time').sort_values('time', ascending=True)

        return hist
    
    def get_funding_history_for_list_of_symbols(self, symbols: List[str], startTime: int=None, endTime:int = None) -> pd.DataFrame:
        """
            Get funding history for a list of symbols
            :param symbols: List of Hyperliquid trading pair symbols
            :param startTime: Start time in milliseconds
            :param endTime: End time in milliseconds. Defaults to current time 

            :return: DataFrame with funding history
        """
        funding_hist = []
        for symbol in symbols:
            localHist = self.get_funding_history(symbol, startTime, endTime)
            if len(localHist) > 0:
                funding_hist.append(localHist)
        funding_hist = pd.concat(funding_hist).reset_index(drop=True)
        return funding_hist