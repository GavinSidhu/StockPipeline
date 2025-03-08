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
    
    # If table exists, check if we have recommendations
    try:
        # Get the latest recommendations
        query = """
        SELECT ticker, date, close, recommendation, min_exploration, max_exploration
        FROM stock.stock_metrics
        WHERE recommendation IS NOT NULL
        ORDER BY ticker, date DESC
        """
        
        recommendations = pd.read_sql(query, engine)
        
        if recommendations.empty:
            message = "📈 **Stock ETL Pipeline Report**\n\nThe pipeline ran successfully, but no trading recommendations were generated based on the 37% rule criteria."
            try:
                context.resources.discord_notifier.send_notification(message=message)
                context.log.info("Sent 'no recommendations' notification to Discord")
                return "Sent 'no recommendations' notification to Discord"
            except Exception as e:
                context.log.error(f"Failed to send Discord notification: {e}")
                return f"Failed to send Discord notification: {e}"
    
        # Create a summary message
        message = "## 📊 **Stock Trading Recommendations**\n\nBased on the 37% rule analysis:"
        
        # Create rich embeds for Discord - one for each ticker
        embeds = []
        
        # Get the latest recommendation for each ticker
        latest_recs = recommendations.groupby('ticker').first().reset_index()
        
        for _, row in latest_recs.iterrows():
            ticker = row['ticker']
            
            # Set color based on action
            color = 0x0000FF  # Blue for HOLD
            if "BUY" in str(row['recommendation']):
                color = 0x00FF00  # Green for BUY
                emoji = "🟢"
            elif "SELL" in str(row['recommendation']):
                color = 0xFF0000  # Red for SELL
                emoji = "🔴"
            else:
                emoji = "🔵"
            
            # Format the title
            title = f"{emoji} {row['recommendation']}"
            
            # Create a description with price details
            description = f"Current price: ${row['close']:.2f}\n"
            
            if pd.notna(row['min_exploration']):
                description += f"Minimum exploration price: ${row['min_exploration']:.2f}\n"
            
            if pd.notna(row['max_exploration']):
                description += f"Maximum exploration price: ${row['max_exploration']:.2f}\n"
            
            # Calculate target prices
            fields = []
            if "BUY" in str(row['recommendation']) and pd.notna(row['min_exploration']):
                target_buy = row['min_exploration'] * 0.95  # 5% below min for extra margin
                fields.append({
                    "name": "Target Buy Price",
                    "value": f"${target_buy:.2f}",
                    "inline": True
                })
            
            if "SELL" in str(row['recommendation']) and pd.notna(row['max_exploration']):
                target_sell = row['max_exploration'] * 1.05  # 5% above max for extra margin
                fields.append({
                    "name": "Target Sell Price",
                    "value": f"${target_sell:.2f}",
                    "inline": True
                })
            
            # Add the date
            fields.append({
                "name": "Date",
                "value": pd.to_datetime(row['date']).strftime('%Y-%m-%d'),
                "inline": True
            })
            
            # Create the embed
            embed = {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields,
                "footer": {
                    "text": "Based on 37% Rule Analysis"
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