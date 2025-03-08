from dagster import asset, Output, MetadataValue
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import io
import numpy as np

@asset(
    deps=["transformed_stock_data"],  # This makes it run after the transformation
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
    
    # Query the metrics table with our 37% rule analysis
    query = """
    SELECT 
        ticker, 
        date, 
        close, 
        recommendation,
        min_exploration,
        max_exploration
    FROM stock.stock_metrics 
    WHERE recommendation IS NOT NULL
    ORDER BY ticker, date DESC
    """
    
    recommendations_df = pd.read_sql(query, engine)
    
    # If no recommendations, create a notification that says so
    if recommendations_df.empty:
        context.log.info("No buy/sell recommendations found")
        return Output(
            pd.DataFrame({"message": ["No buy/sell recommendations at this time"]}),
            metadata={
                "preview": MetadataValue.md("## No Trading Signals\nNo buy/sell recommendations at this time.")
            }
        )
    
    # Get the latest data points for each ticker
    latest_data = recommendations_df.groupby('ticker').first().reset_index()
    
    # Format results in a readable way
    summary = []
    plots = {}
    
    for _, row in latest_data.iterrows():
        ticker = row['ticker']
        
        # Formulate a recommendation with target prices
        if 'BUY' in row['recommendation']:
            action = f"BUY {ticker}"
            min_price = row['min_exploration']
            message = f"Consider buying {ticker} at the current price of ${row['close']:.2f}. "
            message += f"This is below the exploration minimum of ${min_price:.2f}."
            target_price = min_price * 0.95  # Set a target 5% below minimum for extra margin
            message += f" Target buy price: ${target_price:.2f}"
        elif 'SELL' in row['recommendation']:
            action = f"SELL {ticker}"
            max_price = row['max_exploration']
            message = f"Consider selling {ticker} at the current price of ${row['close']:.2f}. "
            message += f"This is above the exploration maximum of ${max_price:.2f}."
            target_price = max_price * 1.05  # Set a target 5% above maximum for extra margin
            message += f" Target sell price: ${target_price:.2f}"
        else:
            action = f"HOLD {ticker}"
            message = f"Hold {ticker} at the current price of ${row['close']:.2f}."
            target_price = None
        
        summary.append({
            "ticker": ticker,
            "action": action,
            "current_price": row['close'],
            "target_price": target_price,
            "message": message
        })
        
        # Get historical data for each ticker to create a plot
        hist_query = f"""
        SELECT date, close, min_exploration, max_exploration
        FROM stock.stock_metrics 
        WHERE ticker = '{ticker}'
        ORDER BY date
        """
        hist_data = pd.read_sql(hist_query, engine)
        
        # Create a plot for this ticker
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(hist_data['date'], hist_data['close'], label='Price')
        
        # Add horizontal lines for min/max exploration values
        if not pd.isna(row['min_exploration']):
            ax.axhline(y=row['min_exploration'], color='g', linestyle='--', label='Min Exploration')
        if not pd.isna(row['max_exploration']):
            ax.axhline(y=row['max_exploration'], color='r', linestyle='--', label='Max Exploration')
        
        ax.set_title(f"{ticker} Price History with 37% Rule Thresholds")
        ax.set_xlabel('Date')
        ax.set_ylabel('Price ($)')
        ax.legend()
        ax.grid(True)
        
        # Save plot to buffer
        buf = io.BytesIO()
        fig.savefig(buf, format='png')
        buf.seek(0)
        
        # Add plot to metadata
        plots[ticker] = buf.getvalue()
    
    # Create a summary DataFrame
    summary_df = pd.DataFrame(summary)
    
    # Create rich metadata with plots and formatted recommendations
    metadata = {
        "summary": MetadataValue.md("## Trading Recommendations\n" + 
                                   "\n".join([f"- {row['message']}" for row in summary])),
    }
    
    # Add plots to metadata
    for ticker, plot_data in plots.items():
        metadata[f"{ticker}_plot"] = MetadataValue.png(plot_data)
    
    return Output(
        summary_df,
        metadata=metadata
    )