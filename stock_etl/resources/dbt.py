from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource

class DbtResource(ConfigurableResource):
    def get_resource(self):
        return DbtCliResource(
            project_dir="/app/stock_etl/dbt_project/finance_dbt",
            profiles_dir="/app/stock_etl/dbt_project",
        )