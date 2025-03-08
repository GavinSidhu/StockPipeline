from dagster import asset, Output, MetadataValue
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

@asset(
    deps=["transformed_stock_data"],
    required_resource_keys={"database_config"}
)
def stock_recommendations(context):
    """Analyze stock data using the 37% rule with the 8 most recent days as exploration for the next 14 days."""
    # Get database config
    db_config = context.resources.database_config
    
    # Connect to database
    engine = create_engine(
        f"postgresql://{db_config.username}:{db_config.password}@"
        f"{db_config.host}:{db_config.port}/{db_config.database}"
    )
    
    # Check if the stock_metrics table exists
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'stock' AND table_name = 'stock_metrics'
            )
        """))
        table_exists = result.scalar()
    
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
    
    # Use the most recent 8 days as the exploration period
    exploration_df = metrics_df[metrics_df['days_from_latest'] < 8].copy()
    context.log.info(f"Exploration period: {exploration_df['date'].min()} to {exploration_df['date'].max()}")
    context.log.info(f"Exploration data points: {len(exploration_df)}")
    
    # Get min/max during this recent exploration phase for each ticker
    exploration_data = exploration_df.groupby('ticker').agg({
        'close': ['min', 'max']
    })
    exploration_data.columns = ['min_exploration', 'max_exploration']
    exploration_data = exploration_data.reset_index()
    
    # Log min/max values for each ticker
    for _, row in exploration_data.iterrows():
        context.log.info(f"Ticker: {row['ticker']}, Min: {row['min_exploration']}, Max: {row['max_exploration']}")
    
    # Most recent price (today) is our reference point
    latest_prices = metrics_df[metrics_df['days_from_latest'] == 0].copy()
    
    # Merge exploration stats with latest prices
    latest_prices = latest_prices.merge(exploration_data, on='ticker', how='left')
    
    # Calculate target prices with multiple thresholds
    latest_prices['buy_strong'] = latest_prices['min_exploration'] * 0.95  # 5% below min
    latest_prices['buy_medium'] = latest_prices['min_exploration'] * 0.98  # 2% below min
    latest_prices['buy_weak'] = latest_prices['min_exploration']  # at min
    
    latest_prices['sell_strong'] = latest_prices['max_exploration'] * 1.05  # 5% above max
    latest_prices['sell_medium'] = latest_prices['max_exploration'] * 1.02  # 2% above max
    latest_prices['sell_weak'] = latest_prices['max_exploration']  # at max
    
    # Add relative positioning vs. min/max
    latest_prices['price_vs_min'] = ((latest_prices['close'] / latest_prices['min_exploration']) - 1) * 100
    latest_prices['price_vs_max'] = ((latest_prices['close'] / latest_prices['max_exploration']) - 1) * 100
    
    # Generate more nuanced forward-looking recommendations
    conditions = [
        (latest_prices['close'] < latest_prices['buy_strong']),  # Strong buy
        (latest_prices['close'] < latest_prices['buy_medium']),  # Medium buy
        (latest_prices['close'] < latest_prices['buy_weak']),    # Weak buy
        (latest_prices['close'] > latest_prices['sell_strong']), # Strong sell
        (latest_prices['close'] > latest_prices['sell_medium']), # Medium sell
        (latest_prices['close'] > latest_prices['sell_weak']),   # Weak sell
        (abs(latest_prices['price_vs_min']) < 1.0),  # Within 1% of min
        (abs(latest_prices['price_vs_max']) < 1.0)   # Within 1% of max
    ]
    choices = [
        'STRONG BUY ' + latest_prices['ticker'] + ' (>5% below exploration min)',
        'MEDIUM BUY ' + latest_prices['ticker'] + ' (2-5% below exploration min)',
        'WEAK BUY ' + latest_prices['ticker'] + ' (0-2% below exploration min)',
        'STRONG SELL ' + latest_prices['ticker'] + ' (>5% above exploration max)',
        'MEDIUM SELL ' + latest_prices['ticker'] + ' (2-5% above exploration max)',
        'WEAK SELL ' + latest_prices['ticker'] + ' (0-2% above exploration max)',
        'WATCH ' + latest_prices['ticker'] + ' - APPROACHING BUY POINT',
        'WATCH ' + latest_prices['ticker'] + ' - APPROACHING SELL POINT'
    ]
    latest_prices['recommendation'] = np.select(conditions, choices, default='HOLD ' + latest_prices['ticker'])
    
    # Add notes explaining the strategy
    latest_prices['notes'] = f"Analysis based on data from {exploration_df['date'].min()} to {exploration_df['date'].max()}. " \
                           f"Current price relative to min: {latest_prices['price_vs_min'].round(2)}%, " \
                           f"to max: {latest_prices['price_vs_max'].round(2)}%."
    
    # Add additional analysis context
    latest_prices['exploration_min'] = latest_prices['min_exploration']
    latest_prices['exploration_max'] = latest_prices['max_exploration']
    latest_prices['current_price'] = latest_prices['close']
    latest_prices['analysis_date'] = max_date
    latest_prices['last_updated'] = pd.Timestamp.now()
    
    # Create a datestamped recommendation table
    date_suffix = max_date.strftime('%Y%m%d')
    latest_prices.to_sql(f"stock_metrics_{date_suffix}", engine, schema='stock', 
                      if_exists='replace', index=False)
    
    # Update the current table
    latest_prices.to_sql('stock_metrics', engine, schema='stock', if_exists='replace', index=False)
    
    context.log.info(f"Created stock_metrics table and stock_metrics_{date_suffix} with {len(latest_prices)} rows")
    
    # Return all recommendations, now including HOLD
    return Output(
        latest_prices,
        metadata={
            "recommendation_count": len(latest_prices[latest_prices['recommendation'].str.contains('BUY|SELL')]),
            "tickers_analyzed": len(latest_prices['ticker'].unique()),
            "exploration_period": f"{exploration_df['date'].min()} to {exploration_df['date'].max()} ({len(exploration_df)} data points)",
            "latest_prices": latest_prices[['ticker', 'close', 'price_vs_min', 'price_vs_max']].to_dict('records')
        }
    )