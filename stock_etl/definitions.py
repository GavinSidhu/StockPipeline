from dagster import Definitions, define_asset_job, ScheduleDefinition, EnvVar
from stock_etl.assets.extract import stock_data
from stock_etl.assets.transform import transformed_stock_data
from stock_etl.assets.analytics import stock_recommendations
from stock_etl.assets.notifications import discord_stock_alert
from stock_etl.resources.db_config import DatabaseConfig
from stock_etl.resources.io_managers import PostgreSQLIOManager
from stock_etl.resources.notifications import discord_notifier
import os 

# Define the job with all assets
weekly_job = define_asset_job(
    name="weekly_stock_etl",
    selection=["stock_data", "transformed_stock_data", "stock_recommendations", "discord_stock_alert"]
)

# Schedule the job to run weekly on Saturday at 5:00 AM
weekly_schedule = ScheduleDefinition(
    job=weekly_job,
    cron_schedule="0 5 * * 6",  # Run at 5:00 AM on Saturday (6=Saturday)
)

# Create the Dagster definitions
defs = Definitions(
    assets=[stock_data, transformed_stock_data, stock_recommendations, discord_stock_alert],
    resources={
        "database_config": DatabaseConfig(),
        "io_manager": PostgreSQLIOManager(config=DatabaseConfig()),
        "discord_notifier": discord_notifier.configured({
            "webhook_url": os.environ.get('DISCORD_WEBHOOK_URL')
        }),
    },
    schedules=[weekly_schedule],
)