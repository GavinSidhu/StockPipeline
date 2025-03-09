from dagster import asset, Output, MetadataValue
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from stock_etl.resources.window_size_config import WindowSizeConfig

@asset(
    deps=["transformed_stock_data"],
    required_resource_keys={"database_config"}
)
def stock_recommendations(context):
    """Analyze stock data using dynamic window sizes based on optimal backtest results."""
    # Get database config
    db_config = context.resources.database_config
    
    # Initialize window size config
    window_config = WindowSizeConfig(db_config)
    
    # Connect to database
    engine = create_engine(
        f"postgresql://{db_config.username}:{db_config.password}@"
        f"{db_config.host}:{db_config.port}/{db_config.database}"
    )
    
    # Get stock data from the database
    stock_data_df = pd.read_sql("SELECT * FROM stock.stock_data", engine)
    
    # Print the column names to debug
    context.log.info(f"Columns in stock_data: {stock_data_df.columns.tolist()}")
    
    # Convert all column names to lowercase for consistency
    stock_data_df.columns = [col.lower() for col in stock_data_df.columns]
    
    # Check if key columns exist, using case-insensitive matching
    column_mapping = {}
    for needed_col in ['ticker', 'date', 'open', 'close']:
        lower_cols = [col.lower() for col in stock_data_df.columns]
        if needed_col in lower_cols:
            column_mapping[needed_col] = stock_data_df.columns[lower_cols.index(needed_col)]
    
    # Check if we have all required columns
    if not all(col in column_mapping for col in ['ticker', 'date', 'open', 'close']):
        missing = [col for col in ['ticker', 'date', 'open', 'close'] if col not in column_mapping]
        context.log.error(f"Missing required columns: {missing}")
        return Output(
            pd.DataFrame(),
            metadata={"error": f"Missing required columns: {missing}"}
        )
    
    # Create a metrics dataframe
    metrics_df = stock_data_df.copy()
    
    # Rename columns for consistency
    metrics_df = metrics_df.rename(columns={
        column_mapping['ticker']: 'ticker',
        column_mapping['date']: 'date',
        column_mapping['open']: 'open',
        column_mapping['close']: 'close'
    })
    
    # Calculate daily change percentage
    metrics_df['daily_change_pct'] = ((metrics_df['close'] - metrics_df['open']) / metrics_df['open'] * 100).round(2)
    
    # Convert date to datetime
    metrics_df['date'] = pd.to_datetime(metrics_df['date'])
    
    # Get the max date to use as reference point for "today"
    max_date = metrics_df['date'].max()
    context.log.info(f"Latest date in data: {max_date}")
    
    # Calculate days from latest data point
    metrics_df['days_from_latest'] = (max_date - metrics_df['date']).dt.days
    
    # Get current window configurations
    window_configs = window_config.get_all_configs()
    context.log.info(f"Using the following window configurations: {window_configs.to_dict('records')}")
    
    # Process each ticker with its own optimal window size
    all_results = []
    
    for ticker in metrics_df['ticker'].unique():
        ticker_data = metrics_df[metrics_df['ticker'] == ticker].copy()
        
        # Get the optimal window size for this ticker (default to 8 if not found)
        window_size = window_config.get_window_size(ticker, default=8)
        context.log.info(f"Using {window_size}-day exploration window for {ticker}")
        
        # Use the optimal window size for the exploration period
        exploration_df = ticker_data[ticker_data['days_from_latest'] < window_size].copy()
        
        # Get min/max during this custom exploration phase
        if not exploration_df.empty:
            min_exploration = exploration_df['close'].min()
            max_exploration = exploration_df['close'].max()
        else:
            context.log.warning(f"No exploration data for {ticker} with {window_size}-day window")
            continue
        
        # Most recent price (today) is our reference point
        latest_price = ticker_data[ticker_data['days_from_latest'] == 0]
        
        if latest_price.empty:
            context.log.warning(f"No latest price for {ticker}")
            continue
            
        latest_price = latest_price.iloc[0]
        
        # Calculate metrics and signals
        result = {
            'ticker': ticker,
            'date': latest_price['date'],
            'close': latest_price['close'],
            'exploration_min': min_exploration,
            'exploration_max': max_exploration,
            'window_size': window_size,
            'price_vs_min': ((latest_price['close'] / min_exploration) - 1) * 100,
            'price_vs_max': ((latest_price['close'] / max_exploration) - 1) * 100,
        }
        
        # Calculate target prices with more realistic thresholds
        result['buy_strong'] = min_exploration * 0.995  # 0.5% below min
        result['buy_medium'] = min_exploration  # at min
        result['buy_weak'] = min_exploration * 1.005  # 0.5% above min
        
        result['sell_strong'] = max_exploration * 1.005  # 0.5% above max
        result['sell_medium'] = max_exploration  # at max
        result['sell_weak'] = max_exploration * 0.995  # 0.5% below max
        
        # Generate more nuanced forward-looking recommendations
        if latest_price['close'] < result['buy_strong']:
            result['recommendation'] = f'STRONG BUY {ticker} (>0.5% below exploration min)'
        elif latest_price['close'] <= result['buy_medium']:
            result['recommendation'] = f'MEDIUM BUY {ticker} (at exploration min)'
        elif latest_price['close'] <= result['buy_weak']:
            result['recommendation'] = f'WEAK BUY {ticker} (slightly above exploration min)'
        elif latest_price['close'] > result['sell_strong']:
            result['recommendation'] = f'STRONG SELL {ticker} (>0.5% above exploration max)'
        elif latest_price['close'] >= result['sell_medium']:
            result['recommendation'] = f'MEDIUM SELL {ticker} (at exploration max)'
        elif latest_price['close'] >= result['sell_weak']:
            result['recommendation'] = f'WEAK SELL {ticker} (slightly below exploration max)'
        elif abs(result['price_vs_min']) < 0.5:
            result['recommendation'] = f'WATCH {ticker} - APPROACHING BUY POINT'
        elif abs(result['price_vs_max']) < 0.5:
            result['recommendation'] = f'WATCH {ticker} - APPROACHING SELL POINT'
        else:
            result['recommendation'] = f'HOLD {ticker}'
        
        # Add notes explaining the strategy
        result['notes'] = f"Analysis based on {window_size}-day exploration window. " \
                          f"Current price relative to min: {result['price_vs_min']:.2f}%, " \
                          f"to max: {result['price_vs_max']:.2f}%."
        
        all_results.append(result)
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(all_results)
    
    # Add additional analysis context
    results_df['analysis_date'] = max_date
    results_df['last_updated'] = pd.Timestamp.now()
    
    # Save to database
    if not results_df.empty:
        # Create a datestamped recommendation table
        date_suffix = max_date.strftime('%Y%m%d')
        results_df.to_sql(f"stock_metrics_{date_suffix}", engine, schema='stock', 
                       if_exists='replace', index=False)
        
        # Update the current table
        results_df.to_sql('stock_metrics', engine, schema='stock', if_exists='replace', index=False)
        
        context.log.info(f"Created stock_metrics table and stock_metrics_{date_suffix} with {len(results_df)} rows")
    
    # Return all recommendations
    return Output(
        results_df,
        metadata={
            "recommendation_count": len(results_df[results_df['recommendation'].str.contains('BUY|SELL')]),
            "tickers_analyzed": len(results_df['ticker'].unique()),
            "window_sizes_used": results_df[['ticker', 'window_size']].to_dict('records'),
            "latest_prices": results_df[['ticker', 'close', 'price_vs_min', 'price_vs_max']].to_dict('records')
        }
    )