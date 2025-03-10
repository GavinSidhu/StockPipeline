from dagster import asset, Output, MetadataValue
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import io
from datetime import datetime, timedelta
import base64

@asset(
    deps=["transformed_stock_data"],
    required_resource_keys={"database_config"}
)
def window_backtest(context):
    """Backtest different exploration window sizes and visualize their performance."""
    # Get database config
    db_config = context.resources.database_config
    
    # Connect to database
    engine = create_engine(
        f"postgresql://{db_config.username}:{db_config.password}@"
        f"{db_config.host}:{db_config.port}/{db_config.database}"
    )
    
    # Get stock data from the database
    stock_data_df = pd.read_sql("SELECT * FROM stock.stock_data", engine)
    
    # Convert all column names to lowercase for consistency
    stock_data_df.columns = [col.lower() for col in stock_data_df.columns]
    
    # Normalize columns
    required_cols = ['ticker', 'date', 'close']
    for col in required_cols:
        if col not in stock_data_df.columns:
            # Try to find a case-insensitive match
            matches = [c for c in stock_data_df.columns if c.lower() == col.lower()]
            if matches:
                stock_data_df = stock_data_df.rename(columns={matches[0]: col})
            else:
                context.log.error(f"Required column {col} not found in data")
                return Output(
                    pd.DataFrame(),
                    metadata={"error": f"Required column {col} not found in data"}
                )
    
    # Convert date to datetime
    stock_data_df['date'] = pd.to_datetime(stock_data_df['date'])
    
    # Sort data
    stock_data_df = stock_data_df.sort_values(['ticker', 'date'])
    
    # Window sizes to test (in trading days)
    window_sizes = [5, 8, 10, 15, 20]
    
    # Creates a dataframe to store backtest results
    results = []
    
    # Defines a timeframe for the backtest
    end_date = stock_data_df['date'].max()
    start_date = stock_data_df['date'].min() 
    
    # Filter data to the backtest period
    backtest_df = stock_data_df.copy()
    
    # Create plots to visualize backtest performance
    all_plots = {}
    
    # Run backtest for each ticker
    for ticker in backtest_df['ticker'].unique():
        ticker_data = backtest_df[backtest_df['ticker'] == ticker].copy()
        
        # Skip if we don't have enough data
        if len(ticker_data) < 30:
            context.log.warning(f"Not enough data for {ticker}, skipping backtest")
            continue
        
        # Create a figure for this ticker's backtest
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Plot the price history
        ax.plot(ticker_data['date'], ticker_data['close'], label='Actual Price', color='black', alpha=0.7)
        
        # Run backtest for each window size
        for window_size in window_sizes:
            # Initialize backtest data
            ticker_backtest = ticker_data.copy()
            ticker_backtest[f'min_{window_size}d'] = ticker_backtest['close'].rolling(window=window_size).min()
            ticker_backtest[f'max_{window_size}d'] = ticker_backtest['close'].rolling(window=window_size).max()
            
            # Decision period is always 14 days for comparison purposes
            decision_period = 14
            
            # Shift the min/max values forward so we're not using future data
            ticker_backtest[f'min_{window_size}d'] = ticker_backtest[f'min_{window_size}d'].shift(1)
            ticker_backtest[f'max_{window_size}d'] = ticker_backtest[f'max_{window_size}d'].shift(1)
            
            # Remove rows with NaN (beginning of the series)
            ticker_backtest = ticker_backtest.dropna(subset=[f'min_{window_size}d', f'max_{window_size}d'])
            
            # Generate signals
            ticker_backtest[f'buy_signal_{window_size}d'] = ticker_backtest['close'] < ticker_backtest[f'min_{window_size}d']
            ticker_backtest[f'sell_signal_{window_size}d'] = ticker_backtest['close'] > ticker_backtest[f'max_{window_size}d']
            
            # Calculate forward returns for performance evaluation
            for days in [1, 3, 5, 10, 14]:
                ticker_backtest[f'fwd_return_{days}d'] = ticker_backtest['close'].pct_change(periods=days).shift(-days) * 100
            
            # Evaluate buy signals
            buy_performance = {}
            for days in [1, 3, 5, 10, 14]:
                # Average return after buy signals
                if ticker_backtest[f'buy_signal_{window_size}d'].any():
                    avg_return = ticker_backtest.loc[ticker_backtest[f'buy_signal_{window_size}d'], f'fwd_return_{days}d'].mean()
                    buy_performance[f'{days}d_return'] = avg_return
                else:
                    buy_performance[f'{days}d_return'] = None
            
            # Evaluate sell signals
            sell_performance = {}
            for days in [1, 3, 5, 10, 14]:
                # Average return after sell signals (negative is good)
                if ticker_backtest[f'sell_signal_{window_size}d'].any():
                    avg_return = ticker_backtest.loc[ticker_backtest[f'sell_signal_{window_size}d'], f'fwd_return_{days}d'].mean()
                    sell_performance[f'{days}d_return'] = avg_return
                else:
                    sell_performance[f'{days}d_return'] = None
            
            # Calculate signal frequency
            buy_count = ticker_backtest[f'buy_signal_{window_size}d'].sum()
            sell_count = ticker_backtest[f'sell_signal_{window_size}d'].sum()
            total_days = len(ticker_backtest)
            
            # Calculate accuracy
            # For buy signals, positive forward return is good
            if buy_count > 0:
                buy_accuracy = (ticker_backtest.loc[ticker_backtest[f'buy_signal_{window_size}d'], f'fwd_return_{decision_period}d'] > 0).mean() * 100
            else:
                buy_accuracy = None
                
            # For sell signals, negative forward return is good
            if sell_count > 0:
                sell_accuracy = (ticker_backtest.loc[ticker_backtest[f'sell_signal_{window_size}d'], f'fwd_return_{decision_period}d'] < 0).mean() * 100
            else:
                sell_accuracy = None
            
            # Store results
            results.append({
                'ticker': ticker,
                'window_size': window_size,
                'buy_count': buy_count,
                'sell_count': sell_count,
                'buy_frequency': buy_count / total_days * 100 if total_days > 0 else 0,
                'sell_frequency': sell_count / total_days * 100 if total_days > 0 else 0,
                'buy_accuracy': buy_accuracy,
                'sell_accuracy': sell_accuracy,
                'avg_buy_return_14d': buy_performance.get('14d_return'),
                'avg_sell_return_14d': sell_performance.get('14d_return'),
            })
            
            # Add signal markers to the plot
            buy_dates = ticker_backtest.loc[ticker_backtest[f'buy_signal_{window_size}d'], 'date']
            buy_prices = ticker_backtest.loc[ticker_backtest[f'buy_signal_{window_size}d'], 'close']
            
            sell_dates = ticker_backtest.loc[ticker_backtest[f'sell_signal_{window_size}d'], 'date']
            sell_prices = ticker_backtest.loc[ticker_backtest[f'sell_signal_{window_size}d'], 'close']
            
            # Plot min/max lines and signals
            ax.plot(ticker_backtest['date'], ticker_backtest[f'min_{window_size}d'], 
                    linestyle='--', alpha=0.5, label=f'Min {window_size}-day')
            ax.plot(ticker_backtest['date'], ticker_backtest[f'max_{window_size}d'], 
                    linestyle='--', alpha=0.5, label=f'Max {window_size}-day')
            
            ax.scatter(buy_dates, buy_prices, marker='^', color='green', label=f'Buy Signal {window_size}-day')
            ax.scatter(sell_dates, sell_prices, marker='v', color='red', label=f'Sell Signal {window_size}-day')
        
        # Finish the plot
        ax.set_title(f'{ticker} - Window Size Backtest (Last 6 Months)')
        ax.set_xlabel('Date')
        ax.set_ylabel('Price ($)')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left')
        
        # Save the plot
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format='png', dpi=120)
        buf.seek(0)
        all_plots[ticker] = buf.getvalue()
        plt.close(fig)
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    # Create a performance summary plot
    if not results_df.empty:
        fig, axs = plt.subplots(2, 2, figsize=(15, 12))
        
        # Flatten axes for easier indexing
        axs = axs.flatten()
        
        # Plot 1: Buy Signal Frequency
        for ticker in results_df['ticker'].unique():
            ticker_results = results_df[results_df['ticker'] == ticker]
            axs[0].plot(ticker_results['window_size'], ticker_results['buy_frequency'], 
                      marker='o', label=ticker)
        axs[0].set_title('Buy Signal Frequency by Window Size')
        axs[0].set_xlabel('Exploration Window Size (Days)')
        axs[0].set_ylabel('Buy Signals (% of Days)')
        axs[0].grid(True, alpha=0.3)
        axs[0].legend()
        
        # Plot 2: Sell Signal Frequency
        for ticker in results_df['ticker'].unique():
            ticker_results = results_df[results_df['ticker'] == ticker]
            axs[1].plot(ticker_results['window_size'], ticker_results['sell_frequency'], 
                      marker='o', label=ticker)
        axs[1].set_title('Sell Signal Frequency by Window Size')
        axs[1].set_xlabel('Exploration Window Size (Days)')
        axs[1].set_ylabel('Sell Signals (% of Days)')
        axs[1].grid(True, alpha=0.3)
        axs[1].legend()
        
        # Plot 3: Buy Signal Accuracy
        for ticker in results_df['ticker'].unique():
            ticker_results = results_df[results_df['ticker'] == ticker]
            axs[2].plot(ticker_results['window_size'], ticker_results['buy_accuracy'], 
                      marker='o', label=ticker)
        axs[2].set_title('Buy Signal Accuracy by Window Size (14-day horizon)')
        axs[2].set_xlabel('Exploration Window Size (Days)')
        axs[2].set_ylabel('Accuracy (%)')
        axs[2].grid(True, alpha=0.3)
        axs[2].legend()
        
        # Plot 4: Sell Signal Accuracy
        for ticker in results_df['ticker'].unique():
            ticker_results = results_df[results_df['ticker'] == ticker]
            axs[3].plot(ticker_results['window_size'], ticker_results['sell_accuracy'], 
                      marker='o', label=ticker)
        axs[3].set_title('Sell Signal Accuracy by Window Size (14-day horizon)')
        axs[3].set_xlabel('Exploration Window Size (Days)')
        axs[3].set_ylabel('Accuracy (%)')
        axs[3].grid(True, alpha=0.3)
        axs[3].legend()
        
        # Save the summary plot
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format='png', dpi=120)
        buf.seek(0)
        all_plots['performance_summary'] = buf.getvalue()
        plt.close(fig)
    
    # Save results to database
    results_df.to_sql('window_backtest_results', engine, schema='stock', if_exists='replace', index=False)
    
    # Create a metadata dictionary with all plots
    metadata = {
        "window_sizes_tested": window_sizes,
        "backtest_period": f"{start_date.date()} to {end_date.date()}"
    }
    
    # Add plots to metadata
    #for ticker, plot_data in all_plots.items():
        #metadata[f"{ticker}_plot"] = MetadataValue.md(f"![{ticker} plot](data:image/png;base64,{base64.b64encode(plot_data).decode()})")
    
    return Output(
        results_df,
        metadata=metadata
    )