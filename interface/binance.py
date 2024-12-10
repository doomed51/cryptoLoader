import requests
import pandas as pd
from typing import List, Dict, Optional
import time
from datetime import datetime, timedelta
import logging 
import duckdb 
from collections import deque 

class binanceDataCollector: 
    def __init__(self, api_key: str = None ):
        """
        Initialize the Binance data collector
        
        :param api_key: Your Binance API key
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

        # self.base_url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
        # self.headers = {
        #     "X-API-KEY": api_key,
        #     "accept": "application/json"
        # }
    
    def get_market_symbols(self): 
        """
        Fetch tickers from Binance
        
        :return: Dataframe of symbols with descriptive cols 
        """
        url = f"https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # return pd.DataFrame(data)
        return pd.DataFrame(data['symbols'])
    
    def get_usdm_perps(self, symbols=None): 
        """
        Get usdm perps for a list of symbols
        
        :return: Dataframe of symbols with descriptive cols 
        """

        if symbols is None: 
            data = self.get_market_symbols()
        else:
            data = symbols

        # select where contractType is PERPETUAL and marginAsset is USDT 
        return data[(data['contractType'] == 'PERPETUAL') & (data['marginAsset'] == 'USDT')]

    def fetch_symbol_data(self, symbol: str, end_point, start_time, end_time, period = '1h', limit = 500): 
        # base_url = "https://fapi.binance.com/fapi/v1/klines"
        base_url = "https://fapi.binance.com/futures/data/"
        url = f"{base_url}{end_point}"

        # make sure start time within last 30 days 
        if start_time < datetime.now() - timedelta(days=30):
            logging.warning("Start time is more than 30 days ago, setting to 30 days ago") 
            start_time = datetime.now() - timedelta(days=30)
        
        # convert start and end times to miliseconds
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        # make sure start time is before end time
        if start_ms > end_ms: 
            logging.error("Start time is after end time")
            return None

        params = {
            "symbol": symbol,
            "period": period,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        data = pd.DataFrame(data)

        # check if data is empty
        if data.empty: 
            logging.warning(f"No data found for {symbol} between {start_time} and {end_time}")
            return None

        # conver columns to numeric
        data['timestamp'] = pd.to_numeric(data['timestamp'])
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
        data['longShortRatio'] = pd.to_numeric(data['longShortRatio'])
        data['longAccount'] = pd.to_numeric(data['longAccount'])
        data['shortAccount'] = pd.to_numeric(data['shortAccount'])
        return data 

    def fetch_multiple_symbol_data(self, symbols: List[str], end_point, start_time, end_time, period = '1h', limit = 500): 
        df_list = []
        for symbol in symbols:
            data = self.fetch_symbol_data(symbol, end_point, start_time, end_time, period, limit)
            if data is not None:
                data['symbol'] = symbol
                df_list.append(data)
                time.sleep(1)
        return pd.concat(df_list)

def main():
    binance_collector = binanceDataCollector()
    # perps = binance_collector.get_usdm_perps()
    
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    # make sure end date mins are 0 
    # end_date = end_date
    start_date = end_date - timedelta(days=22)

    accountRatio = binance_collector.fetch_symbol_data('BTCUSDT', 'topLongShortAccountRatio', start_date, end_date)
    positionRatio = binance_collector.fetch_symbol_data('BTCUSDT', 'topLongShortPositionRatio', start_date, end_date)

    
    # ohlcv = binance_collector.get_ohlcv_for_list_of_symbols(tickers.sort_values('vol24h', ascending=False).head(5)['instId'].tolist())

    # print(ohlcv.head())

if __name__ == "__main__":
    main()