import requests
import pandas as pd
from typing import List, Dict, Optional
import time
from datetime import datetime, timedelta
import logging 
import duckdb 
from collections import deque 

class okxDataCollector: 
    def __init__(self, api_key: str = None ):
        """
        Initialize the OKX data collector
        
        :param api_key: Your OKX API key
        """
        # configure logging 
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.rate_limit_window = 2 
        self.max_requests = 20 
        self.request_timestamps = deque(maxlen=self.max_requests)

        self.base_url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
        self.headers = {
            "X-API-KEY": api_key,
            "accept": "application/json"
        }
    
    def _check_rate_limit(self):
        """Ensure we don't exceed 20 requests per 2 seconds"""
        now = datetime.now()
        while len(self.request_timestamps) >= self.max_requests:
            window_start = now - timedelta(seconds=self.rate_limit_window)
            if self.request_timestamps[0] < window_start:
                self.request_timestamps.popleft()
            else:
                sleep_time = (self.request_timestamps[0] + timedelta(seconds=self.rate_limit_window) - now).total_seconds()
                self.logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(max(0, sleep_time))
                now = datetime.now()
        self.request_timestamps.append(now)
        
    def getTickers(self, instType='SPOT', underlying:str=None, instFamily:str = None): 
        """
        Fetch tickers from OKX
        
        :param instType: Instrument type (SPOT, FUTURES, SWAP)
        :param underlying: Underlying asset
        :param instFamily: Instrument family
        :return: List of tickers
        """
        url = f"https://www.okx.com/api/v5/market/tickers?instType={instType}"
        if underlying: 
            url += f"&underlying={underlying}"
        if instFamily: 
            url += f"&instFamily={instFamily}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return pd.DataFrame(response.json().get('data', []))
        else:
            print(f"Error fetching tickers: {response.status_code}")
            return []

    def get_ohlcv(self, symbol: str, interval: str = '1H', limit: int = 100, after = None) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data for a specific token
        
        :param symbol: OKX trading pair symbol
        :param interval: Candle interval (1d, 1h, etc.)
        :param limit: Number of historical candles to fetch. maximum is 100 
        :param after: Pagination of data to return records earlier than the requested ts
        :return: Pandas DataFrame with OHLCV data
        """
        # if after is None set it to current ts (miliseconds)
        if after is None:
            after = int(datetime.now().timestamp()) * 100
        url = f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&bar={interval}&limit={limit}&after={after}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            hist =  pd.DataFrame(response.json().get('data', []))
            if not hist.empty:
                hist.columns = ['ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm']
                hist['ts'] = pd.to_datetime(pd.to_numeric(hist['ts']), unit='ms')
            else: 
                self.logger.info(f"No OHLCV data for {symbol}-{interval} before {after}")
            # hist.set_index('ts', inplace=True)
            return hist
        else:
            print(f"Error fetching OHLCV data: {response.status_code}")
            return []
    
    # def get_ohlcv_for_list_of_symbols(self, symbols: List[str], interval: str = '1H', limit: int = 100) -> pd.DataFrame:
    #     """
    #     Fetch OHLCV data for a list of symbols
        
    #     :param symbols: List of OKX trading pair symbols
    #     :param interval: Candle interval (1d, 1h, etc.)
    #     :param limit: Number of historical candles to fetch
    #     :return: Pandas DataFrame with OHLCV data
    #     """
    #     df = pd.DataFrame()
    #     after = None 
    #     for symbol in symbols:
    #         self._check_rate_limit()
    #         data = self.get_ohlcv(symbol, interval, limit)
    #         if data is not None:
    #             data['symbol'] = symbol
    #             df = pd.concat([df, data])
    #             time.sleep(1)
    #     return df
    
    def get_ohlcv_for_list_of_symbols(
    self, 
    symbols: List[str], 
    interval: str = '1H', 
    limit: int = 100,
    days_history: int = 30
    ) -> pd.DataFrame:
        """
        Fetch complete OHLCV history for a list of symbols
        
        :param symbols: List of OKX trading pair symbols
        :param interval: Candle interval (1d, 1h, etc.)
        :param limit: Number of candles per request (max 100)
        :param days_history: Number of days of historical data to fetch
        :return: Pandas DataFrame with OHLCV data
        """
        df_list = []
        
        for symbol in symbols:
            self.logger.info(f"Fetching historical data for {symbol}")
            symbol_data = []
            after = int(datetime.now().timestamp() * 1000)  # Current time in milliseconds
            
            while True:
                self._check_rate_limit()
                batch = self.get_ohlcv(symbol, interval, limit, after)
                
                # Replace existing break condition
                if isinstance(batch, list) and not batch:
                    self.logger.info(f"No more data for {symbol}")
                    break
                if isinstance(batch, pd.DataFrame) and batch.empty:
                    self.logger.info(f"No more data for {symbol}")
                    break
                self.logger.info(f"Received {len(batch)} records for {symbol} starting at ts {batch['ts'].min()}")
                symbol_data.append(batch)
                
                # Update after timestamp from oldest record
                oldest_ts = batch['ts'].min()
                after = int(oldest_ts.timestamp() * 1000)
                
                # Stop if we've reached desired history
                if oldest_ts < datetime.now() - timedelta(days=days_history):
                    break
            
            if symbol_data:
                # Combine all batches for this symbol
                symbol_df = pd.concat(symbol_data, ignore_index=True)
                symbol_df['symbol'] = symbol
                df_list.append(symbol_df)
                
        return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def main():
    okx_collector = okxDataCollector()
    tickers = okx_collector.getTickers()
    print(tickers.sort_values('vol24h', ascending=False).head(5))
    
    ohlcv = okx_collector.get_ohlcv_for_list_of_symbols(tickers.sort_values('vol24h', ascending=False).head(5)['instId'].tolist())

    print(ohlcv.head())

if __name__ == "__main__":
    main()