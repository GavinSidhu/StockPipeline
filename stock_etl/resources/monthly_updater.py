from dagster import asset, Output, MetadataValue
from stock_etl.resources.window_size_config import WindowSizeConfig
from stock_etl.resources.threshold_config import ThresholdConfig
import pandas as pd
import datetime

@asset(
    required_resource_keys={"database_config", "discord_notifier"}
)
def update_monthly_params(context):
    """Update window sizes and thresholds on a monthly basis."""
    # Get database config
    db_config = context.resources.database_config
    
    # Initialize window size and threshold configs
    window_config = WindowSizeConfig(db_config)
    threshold_config = ThresholdConfig(db_config)
    
    # Get current month and year for logging
    now = datetime.datetime.now()
    current_month = now.strftime('%B %Y')
    
    context.log.info(f"Running monthly parameter updates for {current_month}")
    
    # Update window sizes
    updated_windows = window_config.update_monthly_if_needed()
    
    # Update thresholds
    updated_thresholds = threshold_config.update_monthly_if_needed()
    
    # Create combined list of all updated tickers
    all_updated = list(set(updated_windows + updated_thresholds))
    
    # Log updates
    context.log.info(f"Updated window sizes for {len(updated_windows)} tickers: {updated_windows}")
    context.log.info(f"Updated thresholds for {len(updated_thresholds)} tickers: {updated_thresholds}")
    
    # Create notification if there were updates
    if all_updated:
        # Get current configurations
        window_configs = window_config.get_all_configs()
        threshold_configs = threshold_config.get_all_configs()
        
        # Join the configs for reporting
        joined_configs = pd.merge(
            window_configs, 
            threshold_configs, 
            on='ticker', 
            suffixes=('_window', '_threshold')
        )
        
        # Format message
        message = f"## 🔄 **Monthly Parameter Update - {current_month}**\n\n"
        message += f"Window sizes and thresholds have been updated for the following tickers: {', '.join(all_updated)}\n\n"
        
        message += "| Ticker | Window Size | Buy Threshold | Sell Threshold | Update Reason |\n"
        message += "|--------|-------------|--------------|----------------|---------------|\n"
        
        for ticker in all_updated:
            config = joined_configs[joined_configs['ticker'] == ticker]
            if not config.empty:
                row = config.iloc[0]
                message += f"| {row['ticker']} | {row['window_size']} days | {row['buy_threshold']}% | "
                message += f"{row['sell_threshold']}% | {row['change_reason_window'] or 'Monthly update'} |\n"
        
        # Send notification
        try:
            context.resources.discord_notifier.send_notification(
                message=message,
                username="Stock ETL Monthly Update"
            )
            context.log.info(f"Sent monthly update notification for {len(all_updated)} tickers")
        except Exception as e:
            context.log.error(f"Failed to send monthly update notification: {e}")
    else:
        context.log.info("No parameters needed updates this month")
    
    # Return results
    return Output(
        pd.DataFrame({
            'month': [current_month],
            'updated_windows_count': [len(updated_windows)],
            'updated_thresholds_count': [len(updated_thresholds)],
            'total_updated_count': [len(all_updated)]
        }),
        metadata={
            "month": current_month,
            "updated_windows": updated_windows,
            "updated_thresholds": updated_thresholds,
            "total_updated": len(all_updated)
        }
    )