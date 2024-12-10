import seaborn as sns 
import indicators.indicators as indicators 
import matplotlib.pyplot as plt
import numpy as np

def _bucketAndCalcSignalReturns(signaldf, signal_col, signal_rounding=1, maxperiod_fwdreturns=20):
    """
        Buckets by signal and calculates the mean fwdReturns for each period 
        inputs:
            signaldf: dataframe with a column to be used as the "signal", and close px to calculate fwd returrns
            signal_col: identifies the column to be used as the signal 
            signal_rounding: (optional) Round the signal column to n decimal places, default is 2
            maxperiod_fwdreturns: (optional) number of fwd returns columns to add to the dataframe, default 2
    """
    
    signaldf.dropna(subset=[signal_col], inplace=True)
    
    # add fwdreturn column for each fwdreturn period
    for i in range(1, maxperiod_fwdreturns+1):
        if 'fwdReturns%s'%(i) in signaldf.columns: # skip if col exists
            continue
        signaldf['fwdReturns%s'%(i)] = signaldf['c'].pct_change(i).shift(-i)
    
    # normalize 
    signaldf['%s_normalized'%(signal_col)] = signaldf[signal_col].round(signal_rounding) 
    fwd_returns_cols = ['fwdReturns{}'.format(i) for i in range(1, maxperiod_fwdreturns + 1)]

    # Perform the groupby and mean calculation in one step
    signal_meanReturns = signaldf.groupby('%s_normalized'%(signal_col))[fwd_returns_cols].mean()
    signal_meanReturns.sort_index(inplace=True, ascending=False) # transpose so that fwdReturns are columns

    return signal_meanReturns

def _apply_default_plot_formatting(ax, title='', xlabel='', ylabel=''):
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

def draw_heatmap_signal_returns(df, ax, colname='logReturn_decile', maxperiod_fwdreturns=20, title='', signal_rounding = 1):
    """
        Draws a heatmap of a signal vs. forward returns.
        Parameters:
        - ax: axes object to draw the heatmap on.
        - y: Column name to use as y-axis data.
        - maxperiod_fwdreturns: Maximum number of periods to calculate forward returns for. Default is 20.
    """
    heatmap = _bucketAndCalcSignalReturns(df, colname, signal_rounding=signal_rounding, maxperiod_fwdreturns=maxperiod_fwdreturns)
    
    # clean up cols
    heatmap.columns = [col.replace('fwdReturns', '') for col in heatmap.columns]
    heatmap = heatmap.iloc[:, :maxperiod_fwdreturns]

    # plot the heatmap
    sns.heatmap(heatmap, ax=ax, cmap='RdYlGn', center=0, annot=False, fmt='.2f')

    # plot formatting
    if title=='':
        _apply_default_plot_formatting(ax, title='%s vs. Forward Returns'%(colname), xlabel='', ylabel='')
    else:
        _apply_default_plot_formatting(ax, title=title, xlabel='', ylabel='')
    ax.set_ylabel(colname)

def plotgrid_decile_returns(df, colname='logReturn_decile', maxperiod_fwdreturns=20, decile_lookback=252):
        """
        Plot a grid of bar plots showing bucketed signal vs. mean forward returns over different periods.
        Works best with a signal bucketed in deciles.
        
        Parameters:
        - colname_y (str): The column name to use for the y-axis of the bar plots. Default is 'logReturn_decile'.
        - maxperiod_fwdreturns (int): The maximum number of periods to calculate forward returns for. Default is 20.
        - decile_lookback (int): Number of periods to use for computing deciles. Default is 252.
        
        Returns:
        - fig: The matplotlib figure object containing the grid of bar plots.
        """
        
        for i in range(1, maxperiod_fwdreturns+1):
            if 'fwdReturns%s'%(i) in df.columns: # skip if col exists
                continue
            df['fwdReturns%s'%(i)] = df['c'].pct_change(i).shift(-i)

        # Create decile column if not exists
        colname_y_decile = f'{colname}_decile'
        if colname_y_decile not in df.columns:
            x, df[colname_y_decile] = indicators.compute_deciles_with_rank(
                df[colname].values, 
                decile_lookback
            )
        
        # Plotting configuration
        num_columns = 5
        num_rows = int(np.ceil(maxperiod_fwdreturns / num_columns))
        
        # Create figure and axes
        fig, axes = plt.subplots(num_rows, num_columns, figsize=(15, 3*num_rows), squeeze=False)
        plt.tight_layout(pad=3.0)
        
        # Iterate through forward return periods
        for i in range(1, maxperiod_fwdreturns + 1):
            row = (i - 1) // num_columns
            col = (i - 1) % num_columns
            
            # Group by decile and calculate mean forward returns
            fwd_return_col = f'fwdReturns{i}'
            ntile_grouped = df.groupby(colname_y_decile)[fwd_return_col].mean()
            
            # Bar plot using Matplotlib
            ax = axes[row, col]
            x = np.arange(len(ntile_grouped))
            ax.bar(x, ntile_grouped.values, align='center', alpha=0.7)
            ax.set_title(f'FwdReturn {i}')
            ax.set_xticks(x)
            ax.set_xticklabels(ntile_grouped.index, rotation=45)
            
            # Optional: Add value labels on top of each bar
            # for j, v in enumerate(ntile_grouped.values):
            #     ax.text(j, v, f'{v:.4f}', ha='center', va='bottom', fontsize=8)
        
        # Hide any unused subplots
        for i in range(maxperiod_fwdreturns, num_rows * num_columns):
            row = i // num_columns
            col = i % num_columns
            fig.delaxes(axes[row, col])
        
        # Set overall figure title
        fig.suptitle(colname, fontsize=14, fontweight='bold')
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        
        return fig