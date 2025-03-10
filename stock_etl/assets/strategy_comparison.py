from dagster import asset, Output, MetadataValue
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import io
import base64
from datetime import datetime, timedelta

@asset(
    deps=["transformed_stock_data"],
    required_resource_keys={"database_config"}
)
def strategy_comparison(context):
    """Compare window-based strategy against regular biweekly buying."""
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
    
    # Ensure we have required columns
    required_cols = ['ticker', 'date', 'close']
    
    # Check if key columns exist, using case-insensitive matching
    column_mapping = {}
    for needed_col in required_cols:
        lower_cols = [col.lower() for col in stock_data_df.columns]
        if needed_col in lower_cols:
            column_mapping[needed_col] = stock_data_df.columns[lower_cols.index(needed_col)]
            
    # Check if we have all required columns
    if not all(col in column_mapping for col in required_cols):
        missing = [col for col in required_cols if col not in column_mapping]
        context.log.error(f"Missing required columns: {missing}")
        return Output(
            pd.DataFrame(),
            metadata={"error": f"Missing required columns: {missing}"}
        )
    
    # Create a clean dataframe
    df = stock_data_df.copy()
    
    # Rename columns for consistency
    df = df.rename(columns={
        column_mapping['ticker']: 'ticker',
        column_mapping['date']: 'date',
        column_mapping['close']: 'close'
    })
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Sort data
    df = df.sort_values(['ticker', 'date'])
    
    # Define the lookback period (6 months)
    end_date = df['date'].max()
    start_date = df['date'].min()
    
    # Filter data to the lookback period
    df_period = df.copy()
    
    # Initialize results dataframe
    results = []
    
    # Test different window sizes
    window_sizes = [5, 8, 10, 15, 20]
    
    # Initialize data for comparative plots
    plot_data = {}
    
    # Process each ticker
    for ticker in df_period['ticker'].unique():
        ticker_data = df_period[df_period['ticker'] == ticker].copy()
        
        # Skip if we don't have enough data
        if len(ticker_data) < 30:
            context.log.warning(f"Not enough data for {ticker}, skipping comparison")
            continue
        
        # Reset initial investment for each strategy
        initial_investment = 10000  # $10,000 starting capital
        biweekly_investment = 500   # $500 every two weeks
        
        # Simulate buy and hold strategy with biweekly investments
        biweekly_returns, biweekly_portfolio = simulate_biweekly_strategy(
            ticker_data, 
            initial_investment=initial_investment,
            biweekly_amount=biweekly_investment
        )
        
        # Store biweekly strategy data for plotting
        plot_data[f"{ticker}_biweekly"] = biweekly_portfolio
        
        # Test each window size
        for window_size in window_sizes:
            # Simulate the window-based strategy
            window_returns, window_portfolio = simulate_window_strategy(
                ticker_data, 
                window_size=window_size,
                initial_investment=initial_investment,
                biweekly_amount=biweekly_investment
            )
            
            # Store window strategy data for plotting
            plot_data[f"{ticker}_window_{window_size}"] = window_portfolio
            
            # Calculate relative performance
            if biweekly_returns['final_value'] > 0:
                relative_performance = (window_returns['final_value'] - biweekly_returns['final_value']) / biweekly_returns['final_value'] * 100
            else:
                relative_performance = 0
                
            # Store results
            results.append({
                'ticker': ticker,
                'strategy': f'Window {window_size}-day',
                'window_size': window_size,
                'initial_value': initial_investment,
                'final_value': window_returns['final_value'],
                'return_pct': window_returns['return_pct'],
                'max_drawdown': window_returns['max_drawdown'],
                'trade_count': window_returns['trade_count'],
                'biweekly_final_value': biweekly_returns['final_value'],
                'biweekly_return_pct': biweekly_returns['return_pct'],
                'biweekly_max_drawdown': biweekly_returns['max_drawdown'],
                'outperformance_pct': relative_performance
            })
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    # Save results to database
    results_df.to_sql('strategy_comparison', engine, schema='stock', if_exists='replace', index=False)
    
    # Create visualizations
    all_plots = {}
    
    # Create a performance comparison plot
    if not results_df.empty:
        # 1. Portfolio value over time for each strategy
        for ticker in df_period['ticker'].unique():
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Plot biweekly strategy
            if f"{ticker}_biweekly" in plot_data:
                biweekly_data = plot_data[f"{ticker}_biweekly"]
                ax.plot(biweekly_data['date'], biweekly_data['portfolio_value'], 
                      label='Buy & Hold (Biweekly)', linewidth=3, color='blue')
            
            # Plot window strategies
            for window_size in window_sizes:
                key = f"{ticker}_window_{window_size}"
                if key in plot_data:
                    window_data = plot_data[key]
                    ax.plot(window_data['date'], window_data['portfolio_value'], 
                          label=f'Window {window_size}-day', alpha=0.7)
            
            ax.set_title(f'{ticker} - Strategy Comparison (Last 6 Months)')
            ax.set_xlabel('Date')
            ax.set_ylabel('Portfolio Value ($)')
            ax.grid(True, alpha=0.3)
            ax.legend()
            
            # Save plot
            buf = io.BytesIO()
            fig.tight_layout()
            fig.savefig(buf, format='png', dpi=120)
            buf.seek(0)
            all_plots[f'{ticker}_comparison'] = buf.getvalue()
            plt.close(fig)
        
        # 2. Summary bar chart of returns by strategy
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Group by ticker and get mean for biweekly strategy (should be same for all window sizes)
        biweekly_returns = results_df.groupby('ticker')['biweekly_return_pct'].first().reset_index()
        biweekly_returns['strategy'] = 'Biweekly Buy & Hold'
        biweekly_returns = biweekly_returns.rename(columns={'biweekly_return_pct': 'return_pct'})
        
        # Combine with window strategy returns
        plot_df = pd.concat([
            results_df[['ticker', 'strategy', 'return_pct']],
            biweekly_returns[['ticker', 'strategy', 'return_pct']]
        ])
        
        # Plot grouped bar chart
        tickers = plot_df['ticker'].unique()
        strategies = sorted(plot_df['strategy'].unique())
        
        bar_width = 0.8 / len(strategies)
        opacity = 0.8
        
        for i, strategy in enumerate(strategies):
            strategy_data = plot_df[plot_df['strategy'] == strategy]
            index = np.arange(len(tickers))
            pos = index - 0.4 + (i + 0.5) * bar_width
            
            # Highlight biweekly strategy
            color = 'blue' if strategy == 'Biweekly Buy & Hold' else None
            
            ax.bar(pos, strategy_data['return_pct'], 
                 width=bar_width, alpha=opacity, label=strategy, color=color)
        
        ax.set_xlabel('Ticker')
        ax.set_ylabel('Return (%)')
        ax.set_title('Return by Strategy and Ticker (6-Month Period)')
        ax.set_xticks(np.arange(len(tickers)))
        ax.set_xticklabels(tickers)
        ax.legend()
        ax.grid(axis='y', alpha=0.3)
        
        # Save plot
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format='png', dpi=120)
        buf.seek(0)
        all_plots['return_comparison'] = buf.getvalue()
        plt.close(fig)
        
        # 3. Outperformance relative to biweekly strategy
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Filter to just window strategies
        window_results = results_df[results_df['strategy'].str.contains('Window')].copy()
        
        # Group by ticker and window size
        for ticker in window_results['ticker'].unique():
            ticker_data = window_results[window_results['ticker'] == ticker]
            ax.plot(ticker_data['window_size'], ticker_data['outperformance_pct'], 
                  marker='o', label=ticker)
        
        ax.axhline(y=0, color='r', linestyle='--', label='Break Even')
        ax.set_xlabel('Window Size (Days)')
        ax.set_ylabel('Outperformance vs Biweekly (%)')
        ax.set_title('Window Strategy Outperformance Relative to Biweekly Buy & Hold')
        ax.grid(True, alpha=0.3)
        ax.legend()
        
        # Save plot
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format='png', dpi=120)
        buf.seek(0)
        all_plots['outperformance'] = buf.getvalue()
        plt.close(fig)
    
    # Prepare metadata
    metadata = {
        "comparison_period": f"{start_date.date()} to {end_date.date()}"
    }
    
    # Convert images to markdown
    for name, img_data in all_plots.items():
        encoded = base64.b64encode(img_data).decode('utf-8')
        metadata[f"{name}_plot"] = MetadataValue.md(f"![{name}](data:image/png;base64,{encoded})")
    
    # Create a markdown summary
    if not results_df.empty:
        markdown = "## Strategy Comparison: Window-Based vs Biweekly Buy & Hold\n\n"
        markdown += f"Analysis period: {start_date.date()} to {end_date.date()}\n\n"
        
        # Summary table
        markdown += "### Return Comparison\n\n"
        markdown += "| Ticker | Strategy | Return (%) | vs Biweekly | Trades |\n"
        markdown += "|--------|----------|------------|-------------|--------|\n"
        
        # Add biweekly rows
        for ticker in results_df['ticker'].unique():
            ticker_rows = results_df[results_df['ticker'] == ticker]
            biweekly_return = ticker_rows['biweekly_return_pct'].iloc[0]
            
            markdown += f"| {ticker} | Biweekly Buy & Hold | {biweekly_return:.2f}% | -- | -- |\n"
            
            # Sort window sizes and add them
            for _, row in ticker_rows.sort_values('window_size').iterrows():
                outperformance = row['outperformance_pct']
                direction = "+" if outperformance >= 0 else ""
                markdown += f"| {ticker} | {row['strategy']} | {row['return_pct']:.2f}% | {direction}{outperformance:.2f}% | {row['trade_count']} |\n"
        
        markdown += "\n\n### Key Insights\n\n"
        
        # Find best strategy for each ticker
        for ticker in results_df['ticker'].unique():
            ticker_rows = results_df[results_df['ticker'] == ticker]
            best_window = ticker_rows.loc[ticker_rows['return_pct'].idxmax()]
            biweekly_return = ticker_rows['biweekly_return_pct'].iloc[0]
            
            # Compare to biweekly
            if best_window['return_pct'] > biweekly_return:
                markdown += f"- **{ticker}**: Window strategy ({best_window['window_size']}-day) outperformed biweekly buy & hold by {best_window['outperformance_pct']:.2f}%\n"
            else:
                markdown += f"- **{ticker}**: Biweekly buy & hold strategy outperformed all window strategies\n"
        
        metadata["summary"] = MetadataValue.md(markdown)
    
    return Output(
        results_df,
        metadata=metadata
    )

def simulate_biweekly_strategy(ticker_data, initial_investment=10000, biweekly_amount=500):
    """Simulate a biweekly buy and hold strategy."""
    # Sort data by date
    ticker_data = ticker_data.sort_values('date')
    
    # Create a copy for simulation
    simulation = ticker_data.copy()
    
    # Add biweekly investment dates (every 14 days)
    first_date = simulation['date'].min()
    biweekly_dates = []
    current_date = first_date
    while current_date <= simulation['date'].max():
        biweekly_dates.append(current_date)
        current_date += timedelta(days=14)
    
    # Initialize portfolio tracking
    simulation['investment'] = 0.0
    simulation['shares_bought'] = 0.0
    simulation['total_shares'] = 0.0
    simulation['portfolio_value'] = 0.0
    
    # Initial investment
    first_idx = simulation.index[0]
    first_price = simulation.loc[first_idx, 'close']
    initial_shares = initial_investment / first_price
    
    simulation.loc[first_idx, 'investment'] = initial_investment
    simulation.loc[first_idx, 'shares_bought'] = initial_shares
    simulation.loc[first_idx, 'total_shares'] = initial_shares
    simulation.loc[first_idx, 'portfolio_value'] = initial_investment
    
    # Process each biweekly date (after the first)
    for date in biweekly_dates[1:]:
        # Find the nearest date in our data
        idx = simulation['date'].searchsorted(date)
        if idx >= len(simulation):
            break
        
        # Get the price on this date
        price = simulation.iloc[idx]['close']
        
        # Calculate shares bought
        shares_bought = biweekly_amount / price
        
        # Update the simulation
        simulation.iloc[idx, simulation.columns.get_loc('investment')] += biweekly_amount
        simulation.iloc[idx, simulation.columns.get_loc('shares_bought')] += shares_bought
    
    # Calculate cumulative values
    total_investment = initial_investment
    total_shares = initial_shares
    
    for i in range(1, len(simulation)):
        # Add new investment and shares
        total_investment += simulation.iloc[i]['investment']
        total_shares += simulation.iloc[i]['shares_bought']
        
        # Update running totals
        simulation.iloc[i, simulation.columns.get_loc('total_shares')] = total_shares
        
        # Calculate portfolio value
        price = simulation.iloc[i]['close']
        portfolio_value = total_shares * price
        simulation.iloc[i, simulation.columns.get_loc('portfolio_value')] = portfolio_value
    
    # Calculate return metrics
    final_value = simulation['portfolio_value'].iloc[-1]
    
    # Account for additional investments
    total_invested = initial_investment + ((len(biweekly_dates) - 1) * biweekly_amount)
    return_pct = (final_value - total_invested) / total_invested * 100
    
    # Calculate max drawdown
    simulation['previous_peak'] = simulation['portfolio_value'].cummax()
    simulation['drawdown'] = (simulation['portfolio_value'] - simulation['previous_peak']) / simulation['previous_peak'] * 100
    max_drawdown = simulation['drawdown'].min()
    
    # Return metrics and portfolio history
    metrics = {
        'final_value': final_value,
        'return_pct': return_pct,
        'max_drawdown': max_drawdown,
        'trade_count': len(biweekly_dates)
    }
    
    return metrics, simulation

def simulate_window_strategy(ticker_data, window_size=8, initial_investment=10000, biweekly_amount=500):
    """Simulate a strategy using the 37% rule with different window sizes."""
    # Sort data by date
    ticker_data = ticker_data.sort_values('date')
    
    # Create a copy for simulation
    simulation = ticker_data.copy()
    
    # Add biweekly cash infusion dates (same as buy & hold for fair comparison)
    first_date = simulation['date'].min()
    biweekly_dates = []
    current_date = first_date
    while current_date <= simulation['date'].max():
        biweekly_dates.append(current_date)
        current_date += timedelta(days=14)
    
    # Initialize portfolio tracking
    simulation['min_window'] = simulation['close'].rolling(window=window_size).min()
    simulation['max_window'] = simulation['close'].rolling(window=window_size).max()
    
    # Shift the min/max values forward so we're not using future data
    simulation['min_window'] = simulation['min_window'].shift(1)
    simulation['max_window'] = simulation['max_window'].shift(1)
    
    # Calculate buy/sell signals
    simulation['buy_signal'] = simulation['close'] < simulation['min_window']
    simulation['sell_signal'] = simulation['close'] > simulation['max_window']
    
    # Fill NaN values at the beginning
    simulation['min_window'] = simulation['min_window'].fillna(simulation['close'])
    simulation['max_window'] = simulation['max_window'].fillna(simulation['close'])
    simulation['buy_signal'] = simulation['buy_signal'].fillna(False)
    simulation['sell_signal'] = simulation['sell_signal'].fillna(False)
    
    # Initialize tracking columns
    simulation['cash'] = 0.0
    simulation['investment'] = 0.0
    simulation['shares'] = 0.0
    simulation['portfolio_value'] = 0.0
    
    # Initial state
    cash = initial_investment
    shares = 0
    trade_count = 0
    
    # Add biweekly cash
    for date in biweekly_dates:
        # Find the nearest date in our data
        idx = simulation['date'].searchsorted(date)
        if idx >= len(simulation):
            break
        
        # Add cash on this date (except first date which is initial investment)
        if date != first_date:
            simulation.iloc[idx, simulation.columns.get_loc('cash')] += biweekly_amount
    
    # Simulate trading day by day
    for i in range(len(simulation)):
        # Add any cash infusion for this day
        cash += simulation.iloc[i]['cash']
        
        # Get price and signals
        price = simulation.iloc[i]['close']
        buy = simulation.iloc[i]['buy_signal']
        sell = simulation.iloc[i]['sell_signal']
        
        # Execute trades
        if buy and cash > 0:
            # Buy with all available cash
            new_shares = cash / price
            shares += new_shares
            
            # Record investment
            simulation.iloc[i, simulation.columns.get_loc('investment')] = cash
            
            # Reset cash
            cash = 0
            trade_count += 1
            
        elif sell and shares > 0:
            # Sell all shares
            cash += shares * price
            
            # Record negative investment (selling)
            simulation.iloc[i, simulation.columns.get_loc('investment')] = -shares * price
            
            # Reset shares
            shares = 0
            trade_count += 1
        
        # Update tracking
        simulation.iloc[i, simulation.columns.get_loc('shares')] = shares
        portfolio_value = (shares * price) + cash
        simulation.iloc[i, simulation.columns.get_loc('portfolio_value')] = portfolio_value
    
    # Calculate return metrics
    final_value = simulation['portfolio_value'].iloc[-1]
    
    # Account for additional investments
    total_invested = initial_investment + ((len(biweekly_dates) - 1) * biweekly_amount)
    return_pct = (final_value - total_invested) / total_invested * 100
    
    # Calculate max drawdown
    simulation['previous_peak'] = simulation['portfolio_value'].cummax()
    simulation['drawdown'] = (simulation['portfolio_value'] - simulation['previous_peak']) / simulation['previous_peak'] * 100
    max_drawdown = simulation['drawdown'].min()
    
    # Return metrics and portfolio history
    metrics = {
        'final_value': final_value,
        'return_pct': return_pct,
        'max_drawdown': max_drawdown,
        'trade_count': trade_count
    }
    
    return metrics, simulation