import os
from dagster import ConfigurableResources

class DatabaseConfig(ConfigurableResource):
    host: str = os.environ.get("DB_HOST", "postgres")
    port: int = int(os.environ.get("DB_PORT", 5432))
    username: str = os.environ.get("DB_USERNAME","postgres")
    password: str = os.environ.get("DB_PASSWORD", "postgres")
    database: str = os.environ.get("DB_DATABASE", "stock_db")