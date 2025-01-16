from .base import BaseExchangeInterface
from .ox import OxDataCollector
from .okx import okxDataCollector
from .binance import binanceDataCollector
from .hyperliquid import hyperliquidDataCollector
import logging

# Define what gets imported with "from interface import *"
__all__ = [
    'BaseExchangeInterface',
    'OxDataCollector',
    'okxDataCollector',
    'binanceDataCollector',
    'hyperliquidDataCollector'
]

# Package metadata
__version__ = '0.1.0'
__author__ = 'rsv'

# Default configuration
DEFAULT_TIMEFRAME = '1h'
DEFAULT_LIMIT = 1000
DEFAULT_RATE_LIMIT = 1.0  # requests per second

# Optional: Initialize logging
logger = logging.getLogger(__name__)