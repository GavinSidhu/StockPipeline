from dagster import asset, AssetExecutionContext
import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import io
import os
import numpy as np
from stock_etl.resources.window_size_config import WindowSizeConfig

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
            #if pd.notna(row['notes']):
                #fields.append({
                    #"name": "Analysis Notes",
                    #"value": row['notes'],
                    #"inline": False
                #})
            
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

@asset(
    deps=["window_backtest"],
    required_resource_keys={"database_config", "discord_notifier"}
)
def backtest_notification(context: AssetExecutionContext):
    """Send Discord notification with window backtest results and auto-adjust window sizes if significant improvement found."""
    # Get database connection
    db_config = context.resources.database_config
    engine = create_engine(
        f"postgresql://{db_config.username}:{db_config.password}@"
        f"{db_config.host}:{db_config.port}/{db_config.database}"
    )
    
    # Initialize window size configuration manager
    window_config = WindowSizeConfig(db_config)
    
    # Check if the backtest results table exists
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'stock' AND table_name = 'window_backtest_results'
            )
        """))
        table_exists = result.scalar()
    
    # If the table doesn't exist, send a notification about the issue
    if not table_exists:
        message = "⚠️ **Backtest Results Missing**\n\nThe backtest job ran but couldn't store results. Check the Dagster logs for details."
        
        try:
            context.resources.discord_notifier.send_notification(message=message)
            context.log.info("Sent issue notification to Discord")
            return "Sent issue notification to Discord"
        except Exception as e:
            context.log.error(f"Failed to send Discord notification: {e}")
            return f"Failed to send Discord notification: {e}"
    
    # If table exists, get the data
    try:
        # Get backtest results
        backtest_df = pd.read_sql("""
            SELECT * FROM stock.window_backtest_results
            ORDER BY ticker, window_size
        """, engine)
        
        if backtest_df.empty:
            message = "📊 **Backtest Analysis**\n\nThe backtest job ran but no results were generated. This might indicate insufficient historical data."
            try:
                context.resources.discord_notifier.send_notification(message=message)
                return "Sent 'no results' notification to Discord"
            except Exception as e:
                context.log.error(f"Failed to send Discord notification: {e}")
                return f"Failed to send Discord notification: {e}"
        
        # Create summary visualizations
        fig, axs = plt.subplots(2, 2, figsize=(15, 12))
        
        # Create a performance score combining accuracy and frequency
        backtest_df['buy_performance'] = backtest_df['buy_accuracy'] * np.sqrt(backtest_df['buy_frequency'])
        backtest_df['sell_performance'] = backtest_df['sell_accuracy'] * np.sqrt(backtest_df['sell_frequency'])
        backtest_df['overall_performance'] = (backtest_df['buy_performance'] + backtest_df['sell_performance']) / 2
        
        # Fill NaN values with 0 for plotting
        backtest_df = backtest_df.fillna(0)
        
        # Plot 1: Buy Signal Performance by Window Size
        for ticker in backtest_df['ticker'].unique():
            ticker_data = backtest_df[backtest_df['ticker'] == ticker]
            axs[0, 0].plot(ticker_data['window_size'], ticker_data['buy_performance'], 
                          marker='o', label=f"{ticker} Buy")
        axs[0, 0].set_title('Buy Signal Performance Score')
        axs[0, 0].set_xlabel('Exploration Window Size (Days)')
        axs[0, 0].set_ylabel('Performance (Accuracy × √Frequency)')
        axs[0, 0].grid(True, alpha=0.3)
        axs[0, 0].legend()
        
        # Plot 2: Sell Signal Performance by Window Size
        for ticker in backtest_df['ticker'].unique():
            ticker_data = backtest_df[backtest_df['ticker'] == ticker]
            axs[0, 1].plot(ticker_data['window_size'], ticker_data['sell_performance'], 
                          marker='o', label=f"{ticker} Sell")
        axs[0, 1].set_title('Sell Signal Performance Score')
        axs[0, 1].set_xlabel('Exploration Window Size (Days)')
        axs[0, 1].set_ylabel('Performance (Accuracy × √Frequency)')
        axs[0, 1].grid(True, alpha=0.3)
        axs[0, 1].legend()
        
        # Plot 3: Overall Performance by Window Size
        for ticker in backtest_df['ticker'].unique():
            ticker_data = backtest_df[backtest_df['ticker'] == ticker]
            axs[1, 0].plot(ticker_data['window_size'], ticker_data['overall_performance'], 
                          marker='o', label=ticker)
        axs[1, 0].set_title('Overall Signal Performance Score')
        axs[1, 0].set_xlabel('Exploration Window Size (Days)')
        axs[1, 0].set_ylabel('Performance Score')
        axs[1, 0].grid(True, alpha=0.3)
        axs[1, 0].legend()
        
        # Plot 4: Table of recommended window sizes
        axs[1, 1].axis('off')
        
        # Save the plot
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format='png', dpi=120)
        buf.seek(0)
        plt.close(fig)
        
        # Auto-adjust window sizes if there's significant improvement
        # Minimum improvement percentage required to change window size (20%)
        min_improvement_pct = 20
        
        # Update window sizes and get list of changes
        changes = window_config.update_from_backtest(backtest_df, min_improvement_pct)
        
        # Current configurations after updates
        current_configs = window_config.get_all_configs()
        
        # Get the current timeframe used for backtest
        #current_timeframe = '6 months'  # Current backtest timeframe
        
        # Create a message with backtest results and changes
        message = f"## 📊 **Weekly Window Size Backtest Results**\n\n"
        message += f"Analysis based on the market data available since fund inception.\n\n"
        
        # Check if we have strategy comparison results
        try:
            strategy_df = pd.read_sql("SELECT * FROM stock.strategy_comparison", engine)
            has_strategy_comparison = not strategy_df.empty
        except:
            has_strategy_comparison = False
        
        # Add strategy comparison summary if available
        if has_strategy_comparison:
            message += f"### 📊 **Strategy Comparison: Window vs Buy & Hold**\n\n"
            message += "Comparing window-based strategies to regular biweekly buying:\n\n"
            message += "| Ticker | Best Window | Window Return | Biweekly Return | Difference |\n"
            message += "|--------|-------------|---------------|-----------------|------------|\n"
            
            for ticker in strategy_df['ticker'].unique():
                ticker_data = strategy_df[strategy_df['ticker'] == ticker]
                best_row = ticker_data.loc[ticker_data['return_pct'].idxmax()]
                
                message += f"| {ticker} | {best_row['window_size']} days | {best_row['return_pct']:.2f}% | "
                message += f"{best_row['biweekly_return_pct']:.2f}% | {best_row['outperformance_pct']:+.2f}% |\n"
            
            message += "\n"
        
        # Add information about automated changes if any
        if changes:
            message += f"### 🤖 **Automated Window Size Adjustments**\n\n"
            message += "The following adjustments were made based on significant performance improvements:\n\n"
            
            message += "| Ticker | Previous Window | New Window | Improvement |\n"
            message += "|--------|----------------|------------|-------------|\n"
            
            for change in changes:
                message += f"| {change['ticker']} | {change['old_window']} days | {change['new_window']} days | {change['improvement_pct']:.1f}% |\n"
            
            message += "\n"
        else:
            message += "### ✓ **No Window Size Changes Needed**\n\n"
            message += "Current window sizes are optimal or no significant improvements were found.\n\n"
        
        # Add table of current window sizes
        message += "### 🔍 **Current Window Size Configuration**\n\n"
        message += "| Ticker | Window Size | Last Updated | Reason |\n"
        message += "|--------|-------------|--------------|--------|\n"
        
        for _, row in current_configs.iterrows():
            update_time = pd.to_datetime(row['last_updated']).strftime("%Y-%m-%d")
            message += f"| {row['ticker']} | {row['window_size']} days | {update_time} | {row['change_reason'] or 'N/A'} |\n"
        
        message += "\n\n### 📈 **Performance Analysis**\n"
        message += "* **Performance Score**: Combines signal accuracy and frequency (higher is better)\n"
        message += f"* **Auto-adjustment**: Window sizes are automatically changed when a {min_improvement_pct}%+ improvement is detected\n"
        message += "* **Timeframe**: This analysis is based on 6 months of historical data\n\n"
        
        # Create embeds with detailed data
        embeds = []
        
        for ticker in backtest_df['ticker'].unique():
            ticker_data = backtest_df[backtest_df['ticker'] == ticker]
            
            # Format detailed data for this ticker
            fields = []
            
            for window_size in sorted(ticker_data['window_size'].unique()):
                window_row = ticker_data[ticker_data['window_size'] == window_size].iloc[0]
                
                # Add field for each window size
                fields.append({
                    "name": f"{window_size}-Day Window",
                    "value": (f"Buy: {window_row['buy_accuracy']:.1f}% accuracy, {window_row['buy_frequency']:.1f}% frequency\n"
                             f"Sell: {window_row['sell_accuracy']:.1f}% accuracy, {window_row['sell_frequency']:.1f}% frequency\n"
                             f"Performance Score: {window_row['overall_performance']:.1f}"),
                    "inline": False
                })
            
            # Get current window size from configs
            current_config = current_configs[current_configs['ticker'] == ticker]
            current_window = current_config['window_size'].iloc[0] if not current_config.empty else 8
            
            # Create embed for this ticker
            best_window = ticker_data.loc[ticker_data['overall_performance'].idxmax(), 'window_size'] \
                if ticker_data['overall_performance'].max() > 0 else current_window
                
            best_score = ticker_data['overall_performance'].max()
            
            # Determine color based on whether a change was made
            color = 0x00FF00 if any(c['ticker'] == ticker for c in changes) else 0x0099FF
            
            embed = {
                "title": f"{ticker} - Window Size Performance Details",
                "description": (f"Current window size: {current_window} days\n"
                               f"Optimal window size: {best_window} days (score: {best_score:.1f})"),
                "color": color,
                "fields": fields,
                "footer": {
                    "text": f"Based on historical data since fund inception"
                }
            }
            
            embeds.append(embed)
        
        # Send the Discord notification
        try:
            # First message with main content and visualization
            context.resources.discord_notifier.send_notification(
                message=message,
                embeds=[]
            )
            
            # Second message with the visualization
            context.resources.discord_notifier.send_notification(
                message="Window Size Performance Analysis:",
                username="Stock ETL Backtest Bot"
            )
            
            # Use requests to send the image directly
            import requests
            webhook_url = context.resources.discord_notifier.webhook_url
            
            files = {
                'file': ('backtest_results.png', buf.getvalue())
            }
            
            requests.post(webhook_url, files=files)
            
            # If we have strategy comparison results, add those too
            try:
                strategy_df = pd.read_sql("SELECT * FROM stock.strategy_comparison", engine)
                if not strategy_df.empty:
                    # Send a message about the strategy comparison
                    context.resources.discord_notifier.send_notification(
                        message="Strategy Comparison: Window vs Buy & Hold:",
                        username="Stock ETL Backtest Bot"
                    )
                    
                    # Get the strategy comparison plots and send them
                    query = "SELECT * FROM dagster_run_asset_materializations WHERE asset_key LIKE '%strategy_comparison%' ORDER BY timestamp DESC LIMIT 1"
                    asset_data = pd.read_sql(query, engine)
                    
                    if not asset_data.empty:
                        # The strategy comparison asset should have created image data
                        # We'd need to extract and send those images, but this is simplified
                        context.log.info("Would send strategy comparison plots here")
            except Exception as e:
                context.log.warning(f"Could not send strategy comparison: {e}")
            
            # Third message with detailed ticker embeds
            context.resources.discord_notifier.send_notification(
                message="Detailed Performance by Ticker:",
                embeds=embeds,
                username="Stock ETL Backtest Bot"
            )
            
            context.log.info("Sent backtest results to Discord")
            return "Sent backtest results to Discord"
        except Exception as e:
            context.log.error(f"Failed to send Discord notification: {e}")
            return f"Failed to send Discord notification: {e}"
            
    except Exception as e:
        # Handle any unexpected errors
        error_message = f"⚠️ **Backtest Analysis Error**\n\nAn error occurred while generating backtest notification: {str(e)}"
        try:
            context.resources.discord_notifier.send_notification(message=error_message)
            context.log.error(f"Error in backtest_notification: {e}")
            return f"Error in backtest_notification: {e}"
        except Exception as notify_error:
            context.log.error(f"Failed to send error notification: {notify_error}")
            return f"Failed to send error notification: {notify_error}"