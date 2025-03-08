from dagster import asset, AssetExecutionContext
import os
import pandas as pd
from sqlalchemy import create_engine
import base64

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
    
    # Get the latest recommendations
    query = """
    SELECT ticker, action, current_price, target_price, message
    FROM stock.stock_recommendations
    ORDER BY ticker
    """
    
    try:
        recommendations = pd.read_sql(query, engine)
    except Exception as e:
        context.log.info(f"No recommendations data available: {e}")
        
        # Send a simple message when no data is available
        try:
            context.resources.discord_notifier.send_notification(
                message="📈 Stock ETL Pipeline ran successfully, but no trading recommendations are available at this time."
            )
            return "Sent 'no recommendations' notification to Discord"
        except Exception as e:
            context.log.error(f"Failed to send Discord notification: {e}")
            return f"Failed to send Discord notification: {e}"
    
    if recommendations.empty:
        # Send a simple message when no recommendations are available
        try:
            context.resources.discord_notifier.send_notification(
                message="📈 Stock ETL Pipeline ran successfully, but no trading recommendations are available at this time."
            )
            return "Sent 'no recommendations' notification to Discord"
        except Exception as e:
            context.log.error(f"Failed to send Discord notification: {e}")
            return f"Failed to send Discord notification: {e}"
    
    # Create a summary message
    message = "## 📊 Stock Trading Recommendations\n"
    
    # Create rich embeds for Discord
    embeds = []
    for _, row in recommendations.iterrows():
        ticker = row['ticker']
        
        # Set color based on action
        color = 0x0000FF  # Blue for HOLD
        if "BUY" in row['action']:
            color = 0x00FF00  # Green for BUY
            emoji = "🟢"
        elif "SELL" in row['action']:
            color = 0xFF0000  # Red for SELL
            emoji = "🔴"
        else:
            emoji = "🔵"
            
        embed = {
            "title": f"{emoji} {row['action']} at ${row['current_price']:.2f}",
            "description": row['message'],
            "color": color,
            "fields": []
        }
        
        if pd.notna(row['target_price']):
            embed["fields"].append({
                "name": "Target Price",
                "value": f"${row['target_price']:.2f}",
                "inline": True
            })
            
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