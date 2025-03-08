from dagster import Definitions, define_asset_job, ScheduleDefinition, EnvVar
from stock_etl.assets.extract import stock_data
from stock_etl.assets.transform import transformed_stock_data
from stock_etl.assets.analytics import stock_recommendations
from stock_etl.assets.notifications import discord_stock_alert
from stock_etl.resources.db_config import DatabaseConfig
from stock_etl.resources.io_managers import PostgreSQLIOManager
from stock_etl.resources.notifications import discord_notifier
import os 

# Define the job that runs all assets
daily_job = define_asset_job(name="daily_stock_etl", selection="*")

# Schedule the job to run at 9 AM Monday through Saturday
daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 9 * * 1-6",  # Run at 9:00 AM Monday through Saturday (1-6)
)

# Create the Dagster definitions
defs = Definitions(
    assets=[stock_data, transformed_stock_data, stock_recommendations, discord_stock_alert],
    resources={
        "database_config": DatabaseConfig(),
        "io_manager": PostgreSQLIOManager(config=DatabaseConfig()),
        "discord_notifier": discord_notifier,
    },
    schedules=[daily_schedule],  # Changed from weekly_schedule to daily_schedule
)