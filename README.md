# cryptoLoader
Load and analyze crypto exchange data with support for multiple exchanges and statistics incl some technical indicators.

## Features
- Fetch OHLCV (Open, High, Low, Close, Volume) data from multiple exchanges
- Helpers for indicators calculation
- Helpers for quick plotting 
- Jupyter notebook examples included

### Supported Exchanges
- OX.FUN (Linear Perpetual Swaps)
- OKX
- Binance
- Birdseye (untested)

### Installation
```sh
git clone https://github.com/doomed51/cryptoLoader 
pip install -r requirements.txt
```

### Project Structure
- helpers - Plotting and visualization utilities
- indicators - Technical analysis indicators
- interface - Exchange API interfaces
- notebooks - Example Jupyter notebooks

### Sample Usage 
1. Create notebook in root cryptoLoader/

2. Initialize data loader
```
from interface import ox
ox_loader = ox.OxDataCollector()
```

3. Load market data 
```
# Get available tickers
tickers = ox_loader.getTickers()

# Load OHLCV data
ohlcv = ox_loader.get_ohlcv_for_list_of_symbols(
    symbols=tickers['marketCode'].tolist(), 
    interval='1H'
)
```

4. add factors / indicators
```
from helpers import plots
import indicators

# Add technical indicators
for symbol in ohlcv['symbol'].unique():
    ohlcv.loc[ohlcv['symbol'] == symbol, 'zscore'] = indicators.zscore(
        ohlcv.loc[ohlcv['symbol'] == symbol, 'close'].values, 
        rollingWindow=24
    )

```

5. Plot 
```
plots.draw_heatmap_signal_returns(ohlcv, colname='logReturn')

```