from dagster import asset, AssetExecutionContext
import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import io
import os
import numpy as np

@asset(
    deps=["stock_recommendations"],
    required_resource_keys={"database_config", "discord_notifier"}
)
def discord_stock_alert(context: AssetExecutionContext):
    """Send Discord alerts with stock recommendations."""
    # Get database connection
    db_config = context.resources.database_config
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
    
    # If the table doesn't exist, send a notification about the issue
    if not table_exists:
        message = "⚠️ **Stock ETL Pipeline Alert**\n\nThe ETL pipeline ran but couldn't generate recommendations because the metrics table doesn't exist yet. This may be fixed in the next run."
        
        try:
            context.resources.discord_notifier.send_notification(message=message)
            context.log.info("Sent issue notification to Discord")
            return "Sent issue notification to Discord"
        except Exception as e:
            context.log.error(f"Failed to send Discord notification: {e}")
            return f"Failed to send Discord notification: {e}"
    
    # If table exists, get the data
    try:
        # Get all stock metrics
        query = """
        SELECT ticker, date, close, recommendation, 
               exploration_min, exploration_max, 
               price_vs_min, price_vs_max,
               buy_strong, buy_medium, buy_weak,
               sell_strong, sell_medium, sell_weak,
               analysis_date, last_updated, notes
        FROM stock.stock_metrics
        ORDER BY ticker
        """
        
        metrics = pd.read_sql(query, engine)
        
        if metrics.empty:
            message = "📈 **Stock ETL Pipeline Report**\n\nThe pipeline ran successfully, but no metrics were generated. This might indicate a data issue."
            try:
                context.resources.discord_notifier.send_notification(message=message)
                context.log.info("Sent 'no metrics' notification to Discord")
                return "Sent 'no metrics' notification to Discord"
            except Exception as e:
                context.log.error(f"Failed to send Discord notification: {e}")
                return f"Failed to send Discord notification: {e}"
        
        # Create a summary message with date information
        analysis_date = pd.to_datetime(metrics['analysis_date'].iloc[0]).strftime('%Y-%m-%d')
        message = f"## 📊 **Stock Market Analysis - {analysis_date}**\n\n"
        message += "Below is today's analysis based on the 37% optimal stopping rule.\n"
        
        # Create rich embeds for Discord - one for each ticker
        embeds = []
        
        for _, row in metrics.iterrows():
            ticker = row['ticker']
            
            # Set color based on recommendation
            if "BUY" in str(row['recommendation']):
                if "STRONG" in str(row['recommendation']):
                    color = 0x00AA00  # Darker green for strong buy
                    emoji = "🟢"
                elif "MEDIUM" in str(row['recommendation']):
                    color = 0x00CC00  # Medium green
                    emoji = "🟢"
                else:
                    color = 0x00FF00  # Light green for weak buy
                    emoji = "🟢"
            elif "SELL" in str(row['recommendation']):
                if "STRONG" in str(row['recommendation']):
                    color = 0xAA0000  # Darker red for strong sell
                    emoji = "🔴"
                elif "MEDIUM" in str(row['recommendation']):
                    color = 0xCC0000  # Medium red
                    emoji = "🔴"
                else:
                    color = 0xFF0000  # Light red for weak sell
                    emoji = "🔴"
            elif "WATCH" in str(row['recommendation']):
                color = 0xFFAA00  # Orange for watch
                emoji = "🟠"
            else:
                color = 0x0000FF  # Blue for hold
                emoji = "🔵"
            
            # Format the title
            title = f"{emoji} {row['recommendation']}"
            
            # Create a description with price details
            description = f"Current price: ${row['close']:.2f}\n"
            description += f"Min price (8-day): ${row['exploration_min']:.2f}\n"
            description += f"Max price (8-day): ${row['exploration_max']:.2f}\n"
            description += f"Relative to min: {row['price_vs_min']:.2f}%\n"
            description += f"Relative to max: {row['price_vs_max']:.2f}%\n"
            
            # Add target prices as fields
            fields = []
            
            # Buy thresholds
            fields.append({
                "name": "Buy Targets",
                "value": f"Strong: ${row['buy_strong']:.2f}\nMedium: ${row['buy_medium']:.2f}\nWeak: ${row['buy_weak']:.2f}",
                "inline": True
            })
            
            # Sell thresholds
            fields.append({
                "name": "Sell Targets",
                "value": f"Strong: ${row['sell_strong']:.2f}\nMedium: ${row['sell_medium']:.2f}\nWeak: ${row['sell_weak']:.2f}",
                "inline": True
            })
            
            # Add notes
            if pd.notna(row['notes']):
                fields.append({
                    "name": "Analysis Notes",
                    "value": row['notes'],
                    "inline": False
                })
            
            # Create the embed
            embed = {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields,
                "footer": {
                    "text": f"Analysis date: {analysis_date} | 37% Rule: 8-day exploration, 14-day decision"
                }
            }
            
            embeds.append(embed)
        
        # Send the Discord notification
        try:
            context.resources.discord_notifier.send_notification(
                message=message,
                embeds=embeds
            )
            context.log.info("Sent recommendation to Discord")
            return "Sent recommendation to Discord"
        except Exception as e:
            context.log.error(f"Failed to send Discord notification: {e}")
            return f"Failed to send Discord notification: {e}"
            
    except Exception as e:
        # Handle any unexpected errors
        error_message = f"⚠️ **Stock ETL Pipeline Error**\n\nAn error occurred while generating recommendations: {str(e)}"
        try:
            context.resources.discord_notifier.send_notification(message=error_message)
            context.log.error(f"Error in discord_stock_alert: {e}")
            return f"Error in discord_stock_alert: {e}"
        except Exception as notify_error:
            context.log.error(f"Failed to send error notification: {notify_error}")
            return f"Failed to send error notification: {notify_error}"