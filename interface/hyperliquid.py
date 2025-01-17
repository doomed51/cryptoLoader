import requests 
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from interface.base import BaseExchangeInterface
import time 
import logging
pd.set_option('future.no_silent_downcasting', True)

REQUEST_MAX_PERIODS = 500 # Responses that take a time range will only return 500 elements or distinct blocks of data
RATE_LIMIT_PER_MINUTE = 60

class hyperliquidDataCollector(BaseExchangeInterface): 
    def __init__(self, api_key: str = None):
        self.base_url = 'https://api.hyperliquid.xyz/'
        self.request_timestamps = []
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
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
                sleep_time = self.request_timestamps[0] + 60 - now + 0.5
                self.logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(max(0, sleep_time))
                now = time.time()
        self.request_timestamps.append(now)

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
        self.check_rate_limit()
        response = requests.post(url, headers=headers, json=params)

        if response.status_code == 200:
            data = response.json().get('universe', [])
            # perps = [d for d in data if d['type'] == 'perpetual']
            data = pd.DataFrame(data)

            data['isDelisted'] = data['isDelisted'].fillna(False)
            data.columns = ['szDecimal', 'symbol', 'maxLeverage', 'isDelisted', 'onlyIsolated']
            return pd.DataFrame(data)
        else:
            print(f"Error fetching perpetual markets: {response.status_code}")
            return []
        
    def get_ohlcv(
        self,
        symbol: str,
        interval: str = '1h',
        limit: int = 5000,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Get OHLCV (Open, High, Low, Close, Volume) candle data from the API.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC')
            interval: Time interval for candles (default: '1h')
            limit: Number of candles to fetch (default: 5000, max: 5000)
            start_time: Start time for fetching candles (optional)
            end_time: End time for fetching candles (optional)
        
        Returns:
            pd.DataFrame: DataFrame containing OHLCV data with columns:
                - timestamp: Unix timestamp in milliseconds
                - open: Opening price
                - high: Highest price
                - low: Lowest price
                - close: Closing price
                - volume: Trading volume
        """
        # API endpoint
        url = 'https://api.hyperliquid.xyz/info'
        
        if end_time is None:
            end_time = datetime.now() 
            # set start time to be 5000 candles before end time
            start_time = end_time - timedelta(hours=limit)


        # Prepare request payload
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": int(start_time.timestamp() * 1000),
                "endTime": int(end_time.timestamp() * 1000)
            }
        }
        
        # print request debug info
        self.logger.info(f"Fetching OHLCV data for {symbol} from {start_time} to {end_time}")


        # Make API request
        self.check_rate_limit()
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Raise exception for bad status codes
        
        # Extract candle data
        candles = response.json()
        
        # Process the response into a DataFrame
        df = pd.DataFrame(candles)
        
        if not df.empty:
            # Rename columns based on API response format
            df = df.rename(columns={
                'T': 'time',
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume'
            })
            # drop columns s, i, n 
            df = df.drop(columns=['s', 'i', 'n'])
            
            # Convert string values to appropriate types
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col])
            
            # Convert timestamp to datetime
            # df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            ## add $ volume column 
            df['dollar_volume'] = df['volume'] * df['close']
            
            # Sort by timestamp
            df = df.sort_values('time', ascending=True)
            
            # Limit the number of rows if specified
            # if limit:
            #     df = df.tail(limit)
        
        return df

    def get_ohlcv_for_symbols(
        self,
        symbols: List[str],
        interval: str = '1h', 
        limit: int = 5000
    ) -> pd.DataFrame:
        """
        Get OHLCV data for multiple symbols
        :param symbols: List of symbols to fetch data for
        :param interval: Time interval for candles (default: '1h')
        :param limit: Number of candles to fetch (default: 100, max: 5000)
        :param days_history: Number of days of history to fetch
        :return: DataFrame with OHLCV data for all symbols
        """
        data = []
        for symbol in symbols:
            # Get OHLCV data for the symbol
            df = self.get_ohlcv(symbol, interval, limit)
            if not df.empty:
                df['symbol'] = symbol
                data.append(df)
            self.logger.info(f"Fetched OHLCV data for {len(data)} out of {len(symbols)} symbols")
        data = pd.concat(data).reset_index(drop=True)
        return data

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
            self.logger.info(f"Fetching funding history for {symbol} from {startTime} to {endTime}")
            
            response = requests.post(url, json=params, headers=headers)

            if response.status_code == 200:
                localHist = pd.DataFrame(response.json())
            else:
                self.logger.error(f"Error fetching funding history for {symbol}: {response.status_code}")
                return hist
            
            # if we have data add it to hist 
            if len(localHist) > 0:
                hist.append(localHist)
            else:
                self.logger.info(f"No more data for {symbol}")
                break

            if len(localHist) < 500:
                self.logger.info(f"No more data for {symbol}")
                break
            
            # update startTime and endTime 
            endTime = int(localHist['time'].min())
            startTime = int(endTime - (REQUEST_MAX_PERIODS * 60 * 60 * 1000))
            
        hist = pd.concat(hist).reset_index(drop=True)
        hist.columns = ['symbol','fundingRate','premium','time']
        # drop any time duplicates
        hist = hist.drop_duplicates(subset='time').sort_values('time', ascending=True)
        hist['fundingRate'] = hist['fundingRate'].astype(float)
        hist['premium'] = hist['premium'].astype(float) 
        self.logger.info(f"Completed fetching funding history for {symbol}")

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
            self.logger.info(f"Fetched funding history for {len(funding_hist)} out of {len(symbols)} symbols")
        funding_hist = pd.concat(funding_hist).reset_index(drop=True)
        return funding_hist