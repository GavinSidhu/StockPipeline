from dagster import asset, Output, MetadataValue
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

@asset(
    deps=["transformed_stock_data"],
    required_resource_keys={"database_config"}
)
def stock_recommendations(context):
    """Analyze stock data and generate buy/sell recommendations."""
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
    
    if not table_exists:
        context.log.info("stock_metrics table doesn't exist, creating it with Python")
        
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
        
        context.log.info(f"Column mapping: {column_mapping}")
        
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
        
        # Calculate rolling 7-day average
        metrics_df['rolling_7d_avg'] = metrics_df.groupby('ticker')['close'].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )
        
        # Apply 37% rule for the last 30 days of data
        metrics_df['date'] = pd.to_datetime(metrics_df['date'])
        
        # Get the max date to use as reference
        max_date = metrics_df['date'].max()
        
        # Add days from latest
        metrics_df['days_from_latest'] = (max_date - metrics_df['date']).dt.days
        
        # Filter to last 30 days
        recent_df = metrics_df[metrics_df['days_from_latest'] <= 30].copy()
        
        # Determine phase (exploration or decision)
        recent_df['phase'] = np.where(recent_df['days_from_latest'] > 19, 'exploration', 'decision')
        
        # Get min/max during exploration phase for each ticker
        exploration_data = recent_df[recent_df['phase'] == 'exploration'].groupby('ticker').agg({
            'close': ['min', 'max']
        })
        exploration_data.columns = ['min_exploration', 'max_exploration']
        exploration_data = exploration_data.reset_index()
        
        # Merge back to get exploration min/max for each ticker
        recent_df = recent_df.merge(exploration_data, on='ticker', how='left')
        
        # Generate buy/sell recommendations
        conditions = [
            (recent_df['phase'] == 'decision') & (recent_df['close'] < recent_df['min_exploration']),
            (recent_df['phase'] == 'decision') & (recent_df['close'] > recent_df['max_exploration'])
        ]
        choices = [
            'CONSIDER BUYING ' + recent_df['ticker'],
            'CONSIDER SELLING ' + recent_df['ticker']
        ]
        recent_df['recommendation'] = np.select(conditions, choices, default=None)
        
        # Save to database
        recent_df.to_sql('stock_metrics', engine, schema='stock', if_exists='replace', index=False)
        
        context.log.info(f"Created stock_metrics table with {len(recent_df)} rows")
        
        # Return a sample of recommendations
        recommendations = recent_df[recent_df['recommendation'].notna()]
        
        return Output(
            recommendations,
            metadata={
                "recommendation_count": len(recommendations),
                "tickers_analyzed": len(recent_df['ticker'].unique()),
                "date_range": f"{recent_df['date'].min()} to {recent_df['date'].max()}"
            }
        )
    else:
        # If table exists, just return a sample
        recommendations = pd.read_sql(
            "SELECT * FROM stock.stock_metrics WHERE recommendation IS NOT NULL", 
            engine
        )
        
        return Output(
            recommendations,
            metadata={
                "recommendation_count": len(recommendations),
                "data_source": "Existing stock_metrics table"
            }
        )