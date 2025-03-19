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
    """Backtest different exploration window sizes and thresholds to find optimal parameters."""
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
    
    # Window sizes to test (in trading days) - expanded range from 14 days to 6 months
    window_sizes = [14, 30, 60, 90, 120, 180]
    
    # More granular threshold percentages to test, better suited for ETFs
    buy_thresholds = [0.5, 1.0, 1.5, 2.0, 3.0]  # From 0.5% to 3%
    sell_thresholds = [0.5, 1.0, 1.5, 2.0, 3.0]  # From 0.5% to 3%
    
    # Creates a dataframe to store backtest results
    results = []
    
    # Define a timeframe for the backtest
    end_date = stock_data_df['date'].max()
    start_date = stock_data_df['date'].min()
    
    context.log.info(f"Running backtest from {start_date} to {end_date}")
    
    # Filter data to the backtest period
    backtest_df = stock_data_df.copy()
    
    # Create plots to visualize backtest performance
    all_plots = {}
    
    # Run backtest for each ticker
    for ticker in backtest_df['ticker'].unique():
        ticker_data = backtest_df[backtest_df['ticker'] == ticker].copy()
        
        # Skip if we don't have enough data (need at least 6 months of data)
        if len(ticker_data) < 180:
            context.log.warning(f"Not enough data for {ticker}, skipping backtest (need at least 180 days)")
            continue
        
        context.log.info(f"Running backtest for {ticker} with {len(ticker_data)} days of data")
        
        # Run backtest for each window size and threshold combination
        for window_size in window_sizes:
            for buy_threshold in buy_thresholds:
                for sell_threshold in sell_thresholds:
                    # Initialize backtest data
                    ticker_backtest = ticker_data.copy()
                    ticker_backtest[f'min_{window_size}d'] = ticker_backtest['close'].rolling(window=window_size).min()
                    ticker_backtest[f'max_{window_size}d'] = ticker_backtest['close'].rolling(window=window_size).max()
                    
                    # Decision period is 14 days for comparison purposes
                    decision_period = 14
                    
                    # Shift the min/max values forward so we're not using future data
                    ticker_backtest[f'min_{window_size}d'] = ticker_backtest[f'min_{window_size}d'].shift(1)
                    ticker_backtest[f'max_{window_size}d'] = ticker_backtest[f'max_{window_size}d'].shift(1)
                    
                    # Remove rows with NaN (beginning of the series)
                    ticker_backtest = ticker_backtest.dropna(subset=[f'min_{window_size}d', f'max_{window_size}d'])
                    
                    # Calculate buy and sell thresholds
                    ticker_backtest[f'buy_threshold_{buy_threshold}'] = ticker_backtest[f'min_{window_size}d'] * (1 + buy_threshold/100)
                    ticker_backtest[f'sell_threshold_{sell_threshold}'] = ticker_backtest[f'max_{window_size}d'] * (1 - sell_threshold/100)
                    
                    # Generate signals based on thresholds
                    ticker_backtest[f'buy_signal'] = ticker_backtest['close'] <= ticker_backtest[f'buy_threshold_{buy_threshold}']
                    ticker_backtest[f'sell_signal'] = ticker_backtest['close'] >= ticker_backtest[f'sell_threshold_{sell_threshold}']
                    
                    # Calculate forward returns for performance evaluation
                    for days in [1, 3, 5, 10, 14]:
                        ticker_backtest[f'fwd_return_{days}d'] = ticker_backtest['close'].pct_change(periods=days).shift(-days) * 100
                    
                    # Evaluate buy signals
                    buy_performance = {}
                    for days in [1, 3, 5, 10, 14]:
                        # Average return after buy signals
                        if ticker_backtest[f'buy_signal'].any():
                            avg_return = ticker_backtest.loc[ticker_backtest[f'buy_signal'], f'fwd_return_{days}d'].mean()
                            buy_performance[f'{days}d_return'] = avg_return
                        else:
                            buy_performance[f'{days}d_return'] = None
                    
                    # Evaluate sell signals
                    sell_performance = {}
                    for days in [1, 3, 5, 10, 14]:
                        # Average return after sell signals (negative is good)
                        if ticker_backtest[f'sell_signal'].any():
                            avg_return = ticker_backtest.loc[ticker_backtest[f'sell_signal'], f'fwd_return_{days}d'].mean()
                            sell_performance[f'{days}d_return'] = avg_return
                        else:
                            sell_performance[f'{days}d_return'] = None
                    
                    # Calculate signal frequency
                    buy_count = ticker_backtest[f'buy_signal'].sum()
                    sell_count = ticker_backtest[f'sell_signal'].sum()
                    total_days = len(ticker_backtest)
                    
                    # Calculate accuracy
                    # For buy signals, positive forward return is good
                    if buy_count > 0:
                        buy_accuracy = (ticker_backtest.loc[ticker_backtest[f'buy_signal'], f'fwd_return_{decision_period}d'] > 0).mean() * 100
                    else:
                        buy_accuracy = None
                        
                    # For sell signals, negative forward return is good
                    if sell_count > 0:
                        sell_accuracy = (ticker_backtest.loc[ticker_backtest[f'sell_signal'], f'fwd_return_{decision_period}d'] < 0).mean() * 100
                    else:
                        sell_accuracy = None
                    
                    # Store results
                    results.append({
                        'ticker': ticker,
                        'window_size': window_size,
                        'buy_threshold': buy_threshold,
                        'sell_threshold': sell_threshold,
                        'buy_count': buy_count,
                        'sell_count': sell_count,
                        'buy_frequency': buy_count / total_days * 100 if total_days > 0 else 0,
                        'sell_frequency': sell_count / total_days * 100 if total_days > 0 else 0,
                        'buy_accuracy': buy_accuracy,
                        'sell_accuracy': sell_accuracy,
                        'avg_buy_return_14d': buy_performance.get('14d_return'),
                        'avg_sell_return_14d': sell_performance.get('14d_return'),
                    })
        
        # Create a visualization for each ticker's best parameters
        ticker_results = pd.DataFrame(results)
        ticker_results = ticker_results[ticker_results['ticker'] == ticker]
        
        if not ticker_results.empty:
            # Calculate a combined performance score
            ticker_results['buy_performance'] = ticker_results['buy_accuracy'] * np.sqrt(ticker_results['buy_frequency'])
            ticker_results['sell_performance'] = ticker_results['sell_accuracy'] * np.sqrt(ticker_results['sell_frequency'])
            ticker_results['overall_performance'] = (ticker_results['buy_performance'] + ticker_results['sell_performance']) / 2
            
            # Get best parameters
            best_params = ticker_results.loc[ticker_results['overall_performance'].idxmax()]
            
            # Create visualization of best parameters
            fig, axs = plt.subplots(2, 2, figsize=(15, 12))
            
            # Plot 1: Window Size Performance
            window_perf = ticker_results.groupby('window_size')['overall_performance'].mean().reset_index()
            axs[0, 0].bar(window_perf['window_size'].astype(str), window_perf['overall_performance'])
            axs[0, 0].set_title(f'{ticker}: Performance by Window Size')
            axs[0, 0].set_xlabel('Window Size (Days)')
            axs[0, 0].set_ylabel('Performance Score')
            axs[0, 0].grid(True, alpha=0.3)
            
            # Plot 2: Buy Threshold Performance
            buy_perf = ticker_results.groupby('buy_threshold')['buy_performance'].mean().reset_index()
            axs[0, 1].bar(buy_perf['buy_threshold'].astype(str) + '%', buy_perf['buy_performance'])
            axs[0, 1].set_title(f'{ticker}: Buy Performance by Threshold')
            axs[0, 1].set_xlabel('Buy Threshold (%)')
            axs[0, 1].set_ylabel('Buy Performance Score')
            axs[0, 1].grid(True, alpha=0.3)
            
            # Plot 3: Sell Threshold Performance
            sell_perf = ticker_results.groupby('sell_threshold')['sell_performance'].mean().reset_index()
            axs[1, 0].bar(sell_perf['sell_threshold'].astype(str) + '%', sell_perf['sell_performance'])
            axs[1, 0].set_title(f'{ticker}: Sell Performance by Threshold')
            axs[1, 0].set_xlabel('Sell Threshold (%)')
            axs[1, 0].set_ylabel('Sell Performance Score')
            axs[1, 0].grid(True, alpha=0.3)
            
            # Plot 4: Table of optimal parameters
            axs[1, 1].axis('off')
            axs[1, 1].text(0.5, 0.9, f'Optimal Parameters for {ticker}', 
                        ha='center', va='center', fontsize=14, fontweight='bold')
            
            param_text = (
                f"Window Size: {best_params['window_size']} days\n"
                f"Buy Threshold: {best_params['buy_threshold']}%\n"
                f"Sell Threshold: {best_params['sell_threshold']}%\n\n"
                f"Buy Accuracy: {best_params['buy_accuracy']:.1f}%\n"
                f"Buy Frequency: {best_params['buy_frequency']:.1f}%\n"
                f"Sell Accuracy: {best_params['sell_accuracy']:.1f}%\n"
                f"Sell Frequency: {best_params['sell_frequency']:.1f}%\n\n"
                f"Overall Score: {best_params['overall_performance']:.1f}"
            )
            
            axs[1, 1].text(0.5, 0.5, param_text, 
                        ha='center', va='center', fontsize=12)
            
            # Save the plot
            buf = io.BytesIO()
            fig.tight_layout()
            fig.savefig(buf, format='png', dpi=120)
            buf.seek(0)
            all_plots[ticker] = buf.getvalue()
            plt.close(fig)
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    # Add performance scores
    if not results_df.empty:
        results_df['buy_performance'] = results_df['buy_accuracy'] * np.sqrt(results_df['buy_frequency'])
        results_df['sell_performance'] = results_df['sell_accuracy'] * np.sqrt(results_df['sell_frequency'])
        results_df['overall_performance'] = (results_df['buy_performance'] + results_df['sell_performance']) / 2
    
    # Create a summary visualization for all tickers
    if not results_df.empty and len(results_df['ticker'].unique()) > 0:
        # Get the best parameters for each ticker
        best_params = []
        
        for ticker in results_df['ticker'].unique():
            ticker_data = results_df[results_df['ticker'] == ticker]
            if not ticker_data.empty and ticker_data['overall_performance'].max() > 0:
                best_row = ticker_data.loc[ticker_data['overall_performance'].idxmax()]
                best_params.append({
                    'ticker': ticker,
                    'window_size': best_row['window_size'],
                    'buy_threshold': best_row['buy_threshold'],
                    'sell_threshold': best_row['sell_threshold'],
                    'overall_performance': best_row['overall_performance']
                })
        
        if best_params:
            best_params_df = pd.DataFrame(best_params)
            
            # Create visualization
            fig, axs = plt.subplots(1, 3, figsize=(18, 6))
            
            # Plot 1: Best Window Size by Ticker
            axs[0].bar(best_params_df['ticker'], best_params_df['window_size'])
            axs[0].set_title('Optimal Window Size by Ticker')
            axs[0].set_xlabel('Ticker')
            axs[0].set_ylabel('Window Size (Days)')
            axs[0].grid(True, alpha=0.3)
            
            # Plot 2: Best Buy Threshold by Ticker
            axs[1].bar(best_params_df['ticker'], best_params_df['buy_threshold'])
            axs[1].set_title('Optimal Buy Threshold by Ticker')
            axs[1].set_xlabel('Ticker')
            axs[1].set_ylabel('Buy Threshold (%)')
            axs[1].grid(True, alpha=0.3)
            
            # Plot 3: Best Sell Threshold by Ticker
            axs[2].bar(best_params_df['ticker'], best_params_df['sell_threshold'])
            axs[2].set_title('Optimal Sell Threshold by Ticker')
            axs[2].set_xlabel('Ticker')
            axs[2].set_ylabel('Sell Threshold (%)')
            axs[2].grid(True, alpha=0.3)
            
            # Save the plot
            buf = io.BytesIO()
            fig.tight_layout()
            fig.savefig(buf, format='png', dpi=120)
            buf.seek(0)
            all_plots['summary'] = buf.getvalue()
            plt.close(fig)
    
    # Save results to database
    if not results_df.empty:
        results_df.to_sql('window_backtest_results', engine, schema='stock', if_exists='replace', index=False)
    
    # Create a metadata dictionary with all plots
    metadata = {
        "window_sizes_tested": window_sizes,
        "buy_thresholds_tested": buy_thresholds,
        "sell_thresholds_tested": sell_thresholds,
        "backtest_period": f"{start_date.date()} to {end_date.date()}"
    }
    
    # Add summary table of best parameters
    if not results_df.empty:
        best_params_summary = []
        
        for ticker in results_df['ticker'].unique():
            ticker_data = results_df[results_df['ticker'] == ticker]
            if not ticker_data.empty and ticker_data['overall_performance'].max() > 0:
                best_row = ticker_data.loc[ticker_data['overall_performance'].idxmax()]
                best_params_summary.append({
                    'ticker': ticker,
                    'window_size': int(best_row['window_size']),
                    'buy_threshold': float(best_row['buy_threshold']),
                    'sell_threshold': float(best_row['sell_threshold']),
                    'overall_performance': float(best_row['overall_performance']),
                    'buy_accuracy': float(best_row['buy_accuracy']) if pd.notna(best_row['buy_accuracy']) else 0,
                    'sell_accuracy': float(best_row['sell_accuracy']) if pd.notna(best_row['sell_accuracy']) else 0
                })
        
        if best_params_summary:
            metadata["best_parameters"] = best_params_summary
    
    return Output(
        results_df,
        metadata=metadata
    )