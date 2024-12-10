import pandas as pd 
from interface.birdseye import BirdeyeDataCollector
import matplotlib.pyplot as plt

# read in api_key.txt 
with open("api_key.txt", "r") as f:
    api_key = f.read().strip()

collector = BirdeyeDataCollector(api_key)

ohlcv_data = collector.collect_top_tokens_ohlcv(limit=50, days=30)

