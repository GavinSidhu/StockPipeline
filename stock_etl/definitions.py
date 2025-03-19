from dagster import Definitions, define_asset_job, ScheduleDefinition
from stock_etl.assets.extract import stock_data
from stock_etl.assets.transform import transformed_stock_data
from stock_etl.assets.analytics import stock_recommendations
from stock_etl.assets.notifications import discord_stock_alert, backtest_notification
from stock_etl.assets.window_backtest import window_backtest
from stock_etl.assets.position_sizing import position_sizing
from stock_etl.assets.strategy_comparison import strategy_comparison
from stock_etl.resources.db_config import DatabaseConfig
from stock_etl.resources.io_managers import PostgreSQLIOManager
from stock_etl.resources.notifications import discord_notifier
from stock_etl.resources.window_size_config import WindowSizeConfig
from stock_etl.resources.threshold_config import ThresholdConfig
from stock_etl.resources.monthly_updater import update_monthly_params

# Define the jobs
daily_job = define_asset_job(
    name="daily_stock_etl",
    selection=["stock_data", "transformed_stock_data", "stock_recommendations", "position_sizing", "discord_stock_alert"]
)

# Define a separate job for backtesting that runs weekly
backtest_job = define_asset_job(
    name="weekly_backtest",
    selection=["stock_data", "transformed_stock_data", "window_backtest", "strategy_comparison", "backtest_notification"]
)

# Define a job for monthly parameter updates
monthly_update_job = define_asset_job(
    name="monthly_parameter_update",
    selection=["update_monthly_params"]
)

# Schedule the daily job to run at 9 AM Monday through Saturday
daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 9 * * 1-6",  # Run at 9:00 AM Monday through Saturday (1-6)
)

# Schedule the backtest job to run once a week on Sunday
backtest_schedule = ScheduleDefinition(
    job=backtest_job,
    cron_schedule="0 10 * * 0"  # Run at 10:00 AM every Sunday
)

# Schedule the monthly parameter update to run on the 1st of each month
monthly_update_schedule = ScheduleDefinition(
    job=monthly_update_job,
    cron_schedule="0 1 1 * *"  # Run at 1:00 AM on the 1st day of each month
)

# Create the Dagster definitions
defs = Definitions(
    assets=[
        stock_data, 
        transformed_stock_data, 
        stock_recommendations, 
        position_sizing,
        window_backtest,
        strategy_comparison,
        discord_stock_alert,
        backtest_notification,
        update_monthly_params
    ],
    resources={
        "database_config": DatabaseConfig(),
        "io_manager": PostgreSQLIOManager(config=DatabaseConfig()),
        "discord_notifier": discord_notifier,
    },
    schedules=[daily_schedule, backtest_schedule, monthly_update_schedule],
)