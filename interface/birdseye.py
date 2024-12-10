import requests
import pandas as pd
from typing import List, Dict, Optional
import time
from datetime import datetime, timedelta
import logging 
import duckdb 

class BirdeyeDataCollector:
    def __init__(self, api_key: str):
        """
        Initialize the Birdeye data collector
        
        :param api_key: Your Birdeye API key
        """
        # configure logging 
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.base_url = "https://public-api.birdeye.so/public/multi_price_v2"
        self.ohlcv_url = "https://public-api.birdeye.so/public/history_price_v2"
        self.headers = {
            "X-API-KEY": api_key,
            "accept": "application/json"
        }
    
    def get_top_tokens(self, limit: int = 100) -> List[Dict]:
        """
        Fetch top tokens by market cap
        
        :param limit: Number of top tokens to retrieve
        :return: List of token dictionaries
        """
        url = f"https://public-api.birdeye.so/public/markets?sort_by=market_cap&sort_type=desc&limit={limit}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return response.json().get('data', {}).get('markets', [])
        else:
            print(f"Error fetching top tokens: {response.status_code}")
            return []
    
    def get_ohlcv_data(
        self, 
        token_address: str, 
        days: int = 30, 
        resolution: str = '1D'
    ) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data for a specific token
        
        :param token_address: Solana token contract address
        :param days: Number of historical days to fetch
        :param resolution: Candle resolution (1D, 1H, etc.)
        :return: Pandas DataFrame with OHLCV data
        """
        end_timestamp = int(datetime.now().timestamp())
        start_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        
        params = {
            "address": token_address,
            "from_timestamp": start_timestamp,
            "to_timestamp": end_timestamp,
            "resolution": resolution
        }
        
        try:
            response = requests.get(
                self.ohlcv_url, 
                headers=self.headers, 
                params=params
            )
            
            if response.status_code == 200:
                data = response.json().get('data', {}).get('items', [])
                
                if not data:
                    print(f"No OHLCV data for {token_address}")
                    return None
                
                df = pd.DataFrame(data)
                df['timestamp'] = pd.to_datetime(df['time'], unit='s')
                df = df.set_index('timestamp')
                
                return df[['open', 'high', 'low', 'close', 'volume']]
            else:
                print(f"Error fetching OHLCV: {response.status_code}")
                return None
        
        except Exception as e:
            print(f"Exception in OHLCV data fetch: {e}")
            return None
    
    def collect_top_tokens_ohlcv(
        self, 
        limit: int = 50, 
        days: int = 30
    ) -> Dict[str, pd.DataFrame]:
        """
        Collect OHLCV data for top tokens
        
        :param limit: Number of top tokens to collect
        :param days: Historical days to fetch for each token
        :return: Dictionary of token addresses and their OHLCV data
        """
        top_tokens = self.get_top_tokens(limit)
        ohlcv_data = {}
        
        for token in top_tokens:
            token_address = token.get('address')
            token_symbol = token.get('symbol', 'UNKNOWN')
            
            print(f"Fetching data for {token_symbol} ({token_address})")
            
            try:
                df = self.get_ohlcv_data(token_address, days=days)
                
                if df is not None and not df.empty:
                    ohlcv_data[token_symbol] = df
                
                # Rate limiting to avoid API restrictions
                time.sleep(0.5)
            
            except Exception as e:
                print(f"Error processing {token_symbol}: {e}")
        
        return ohlcv_data

    def save_token_ohlcv_data(
        self, 
        token_symbol: str, 
        df: pd.DataFrame,
        db_path: str = "birdeye_data.db"
    ) -> bool:
        """
        Save token OHLCV to unified table in duckdb database

        :param token_symbol: Token symbol
        :param df: Pandas DataFrame with OHLCV data
        :param db_path: Path to DuckDB database file
        :return: True if successful, False otherwise
        """
        try:
            # Add token symbol column
            df['token_symbol'] = token_symbol
            
            conn = duckdb.connect(db_path)
            
            # Create table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS token_ohlcv (
                    token_symbol VARCHAR,
                    timestamp TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    PRIMARY KEY (token_symbol, timestamp)
                )
            """)
            
            # Register temporary view
            conn.register('new_data', df)
            
            # Upsert data
            conn.execute("""
                INSERT OR REPLACE INTO token_ohlcv 
                SELECT * FROM new_data
            """)
            
            conn.close()
            self.logger.info(f"Saved {token_symbol} OHLCV data to {db_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving {token_symbol} data: {e}")
            return False

def main():
    # Replace with your actual Birdeye API key
    API_KEY = 'YOUR_BIRDEYE_API_KEY'
    
    collector = BirdeyeDataCollector(API_KEY)
    
    # Collect OHLCV data for top 50 tokens for last 30 days
    token_data = collector.collect_top_tokens_ohlcv(limit=50, days=30)
    
    # Example: Save each token's data to a CSV
    for symbol, df in token_data.items():
        df.to_csv(f"{symbol}_ohlcv.csv")
        print(f"Saved {symbol} OHLCV data")

if __name__ == "__main__":
    main()