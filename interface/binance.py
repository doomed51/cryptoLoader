import requests
import pandas as pd
from typing import List, Dict, Optional
import time
from datetime import datetime, timedelta
import logging 
import duckdb 
from collections import deque 
import os

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

    def fetch_symbol_kline(self, symbol: str, start_time, end_time, period = '1h', limit = 1500):
        """
            api ref: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data#http-request
        
        """
        # base_url = "https://fapi.binance.com/fapi/v3/klines"
        base_url = "https://api.binance.com/api/v3/klines"
        url = f"{base_url}"

        if not start_time is None:
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
            "interval": period,
            # "startTime": start_ms,
            # "endTime": end_ms,
            "limit": limit
        }
        try:
            response = requests.get(url, params=params)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return None
        response.raise_for_status()
        data = response.json()
        data = pd.DataFrame(data)

        # check if data is empty
        if data.empty: 
            logging.warning(f"No data found for {symbol} between {start_time} and {end_time}")
            return None
        
        ## Response format
        #[
        #   [
        #     1499040000000,      // Open time
        #     "0.01634790",       // Open
        #     "0.80000000",       // High
        #     "0.01575800",       // Low
        #     "0.01577100",       // Close
        #     "148976.11427815",  // Volume
        #     1499644799999,      // Close time
        #     "2434.19055334",    // Quote asset volume
        #     308,                // Number of trades
        #     "1756.87402397",    // Taker buy base asset volume
        #     "28.46694368",      // Taker buy quote asset volume
        #     "17928899.62484339" // Ignore.
        #   ]
        # ]

        # convert cols to numeric 
        data.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'num_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        data['timestamp'] = pd.to_numeric(data['timestamp'])
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
        data['open'] = pd.to_numeric(data['open'])
        data['high'] = pd.to_numeric(data['high'])
        data['low'] = pd.to_numeric(data['low'])
        data['close'] = pd.to_numeric(data['close'])
        data['volume'] = pd.to_numeric(data['volume'])
        data['close_time'] = pd.to_numeric(data['close_time'])
        data['quote_asset_volume'] = pd.to_numeric(data['quote_asset_volume'])
        data['num_trades'] = pd.to_numeric(data['num_trades'])
        data['taker_buy_base_asset_volume'] = pd.to_numeric(data['taker_buy_base_asset_volume'])
        data['taker_buy_quote_asset_volume'] = pd.to_numeric(data['taker_buy_quote_asset_volume'])
        # data['ignore'] = pd.to_numeric(data['ignore'])

        return data
    def check_for_perp_history_csv(self):
        # check for perp_history_<DATE>.csv in current folder 
        filename = f"perp_history_{datetime.now().strftime('%Y-%m-%d')}.csv"
        if os.path.exists(filename):
            return True
        else:
            return False
    
    def fetch_multiple_symbol_kline(self, symbols: List[str], start_time=None, end_time=None, period = '1h', limit = 1500):
        # https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
        
        df_list = []
        i=0
        for symbol in symbols:
            i+=1 
            logging.info(f"Fetching data for {symbol} ({i}/{len(symbols)})")
            try: 
                data = self.fetch_symbol_kline(symbol, start_time, end_time, period, limit)
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed: {e}")
                continue
            if data is not None:
                data['symbol'] = symbol
                df_list.append(data)
                time.sleep(1)
        if self.check_for_perp_history_csv() == False:
            # save data to csv
            df = pd.concat(df_list)
            df.to_csv(f"perp_history_{datetime.now().strftime('%Y-%m-%d')}.csv", index=False)
            
        return pd.concat(df_list)

def main():
    binance_collector = binanceDataCollector()
    # perps = binance_collector.get_usdm_perps()
    
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    # make sure end date mins are 0 
    # end_date = end_date
    start_date = end_date - timedelta(days=29)

    # accountRatio = binance_collector.fetch_symbol_data('BTCUSDT', 'topLongShortAccountRatio', start_date, end_date)
    # positionRatio = binance_collector.fetch_symbol_data('BTCUSDT', 'topLongShortPositionRatio', start_date, end_date)

    klines = binance_collector.fetch_symbol_kline('BTCUSDT', start_date, end_date)
    print(klines)

    
    # ohlcv = binance_collector.get_ohlcv_for_list_of_symbols(tickers.sort_values('vol24h', ascending=False).head(5)['instId'].tolist())

    # print(ohlcv.head())

if __name__ == "__main__":
    main()