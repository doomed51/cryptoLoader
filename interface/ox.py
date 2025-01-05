"""
    Interface for ox.fun 
"""

import requests
import pandas as pd
from typing import List, Dict, Optional
import logging
import time

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
        """Ensure we don't exceed 100 requests per second"""
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
    
    def getTickers(self):
        """
            Fetch list of markets on OX
        """
        url = f"{self.base_url}markets"
        self.check_rate_limit()
        response = requests.get(url)
        response.raise_for_status()
        # return response.json()
        return pd.DataFrame(response.json().get('data', []))