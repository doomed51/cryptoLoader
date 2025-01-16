from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime

class BaseExchangeInterface(ABC):
    @abstractmethod
    def __init__(self, api_key: str = None):
        """Initialize exchange interface"""
        pass
    
    @abstractmethod
    def get_tickers(self) -> pd.DataFrame:
        """Get current market tickers"""
        pass

    @abstractmethod
    def get_markets(self) -> pd.DataFrame:
        """Get available markets"""
        pass
        
    @abstractmethod
    def get_ohlcv(
        self,
        symbol: str,
        interval: str = '1h',
        limit: int = 100,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Get OHLCV candle data"""
        pass

    @abstractmethod 
    def get_ohlcv_for_symbols(
        self,
        symbols: List[str],
        interval: str = '1h', 
        limit: int = 100,
        days_history: int = 30
    ) -> pd.DataFrame:
        """Get OHLCV data for multiple symbols"""
        pass