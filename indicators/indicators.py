import pandas as pd
import numpy as np

from numba import njit
from numpy.lib.stride_tricks import sliding_window_view

@njit
def exponentially_weighted_moving_average(values, length, alpha):
    """
    Calculates the exponentially weighted moving average of a series.
    inputs:
        arr: array of prices
        length: lookback period
        alpha: smoothing factor for calculating weights. lower alpha gives more weight to recent prices
    """
    weights = np.array([(1 - alpha) * (alpha ** i) for i in range(length)])[::-1]
    # print(weights)
    result = np.empty(len(values))

    for i in range(len(result)):
        # print(values[i])
        # print last 10 values in values 
        if i < length:
            result[i] = np.nan
        else:
            # print(values[i-length:i])
            result[i] = np.dot(values[i-length:i], weights) / weights.sum()
    
    # print('\n')
    # print(weights) 
    # print(len(values))
    
    return result

@njit 
def compute_deciles_with_rank(values, window_size):   
    """
        Calculate the deciles of a series of values.
    """
    windows = sliding_window_view(values, window_size)

    rolling_deciles = np.empty((windows.shape[0], 9))
    decile_ranks = np.empty(len(values))
    for i in range(windows.shape[0]):
        # deciles[i, :] = compute_deciles(windows[i])
        rolling_deciles[i, :] = np.percentile(windows[i], np.arange(10, 100, 10))
        current_value = values[i + window_size - 1]
        decile_ranks[i + window_size - 1] = find_decile(current_value, rolling_deciles[i])

    decile_ranks[:window_size - 1] = np.nan
    # return np.percentile(values, window, np.arange(10, 100, 10))
    return rolling_deciles, decile_ranks

@njit
def find_decile(value, deciles): 
    """
        Find the decile of a value given a list of deciles.
    """
    for i, decile in enumerate(deciles):
        if value <= decile:
            return i
    return 9

@njit
def momentum(prices, lookback):
    n = len(prices)
    momo = np.empty(n)
    
    for i in range(n):
        if i >= lookback:
            momo[i] = (prices[i] / prices[i - lookback]) - 1
        else:
            momo[i] = np.nan
    
    return momo

@njit
def momentum2(prices, lookback):
    n = len(prices)
    momo = np.empty(n)
    
    # Initialize arrays with NaN
    momo[:] = np.nan
    
    # Find the first non-NaN index
    start_idx = 0
    for i in range(n):
        if not np.isnan(prices[i]):
            start_idx = i
            break
    
    for i in range(start_idx + lookback, n):
        momo[i] = (prices[i] / prices[i - lookback]) - 1
    
    return momo

@njit
def rolling_zscore(values, rollingWindow):
    n = len(values)
    zscores = np.empty(n)
    zscores[:] = np.nan  # Initialize with NaNs

    for i in range(rollingWindow - 1, n):
        window = values[i - rollingWindow + 1:i + 1]
        mean = np.mean(window)
        std = np.std(window)
        zscores[i] = (values[i] - mean) / std if std!= 0 else 0

    return zscores

@njit
def zscore(values, rollingWindow=252, rescale=False):
    """
    Calculate the z-score of a column.
    Params: 
        colname: str column name to calculate z-score on
        rollingWindow: int rolling window to calculate z-score on. Setting to 0 uses entire population 
        _pxHistory: pd.DataFrame to calculate z-score on. Default is None, which uses the objects default pxhistory
    """
    
    if rollingWindow == 0:
        zscores = (values - np.mean(values)) / np.std(values)
    else:
        zscores = rolling_zscore(values, rollingWindow)

    # if rescale:
    #     zscores = ffn.rescale(zscores, -1, 1)

    return zscores

@njit
def rolling_std(arr, window):
    """
    Calculate the rolling standard deviation of a column.
    Params: 
        arr: np.array column to calculate rolling std on
        window: int rolling window 
    """
    result = np.empty(len(arr))
    result[:] = np.nan  # Initialize with NaNs
    for i in range(window - 1, len(arr)):
        result[i] = np.std(arr[i - window + 1:i])
    return result

@njit
def _calc_fwd_returns(prices: np.array, maxperiod: int = 20) -> np.ndarray:
    """
    Calculate forward returns for multiple periods
    
    Args:
        prices: numpy array of prices
        maxperiod: maximum forward period to calculate
    Returns:
        2D array of forward returns [periods x prices]
    """
    n = len(prices)
    fwd_returns = np.empty((maxperiod, n))
    fwd_returns[:] = np.nan
    
    for period in range(1, maxperiod + 1):
        for i in range(n - period):
            fwd_returns[period-1, i] = (prices[i + period] / prices[i]) - 1
            
    return fwd_returns

def calc_fwd_returns(df: pd.DataFrame, maxperiod:int =20):
    """
    Calculate forward returns for multiple periods
    
    Args:
        df: pandas DataFrame with prices
        maxperiod: maximum forward period to calculate
    Returns:
        pandas DataFrame with forward returns
    """
    prices = df['close'].values
    fwd_returns = _calc_fwd_returns(prices, maxperiod)

    ## add the columns to the dataframe
    for period in range(1, maxperiod + 1):
        df[f'fwdReturns_{period}'] = fwd_returns[period-1]
    
    return df 