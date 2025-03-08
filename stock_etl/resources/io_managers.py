from dagster import IOManager, OutputContext, InputContext
import pandas as pd
from sqlalchemy import create_engine

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self.config = config
        
    def handle_output(self, context: OutputContext, obj):
        if isinstance(obj, pd.DataFrame) and not obj.empty:
            engine = create_engine(
                f"postgresql://{self.config.username}:{self.config.password}@"
                f"{self.config.host}:{self.config.port}/{self.config.database}"
            )
            table_name = f"stock.{context.asset_key.path[-1]}"
        
            # Create a temporary table with the new data
            temp_table = f"{table_name}_temp"
            obj.to_sql(temp_table, engine, if_exists="replace", index=False)
        
            #I nsert into the main table, avoiding duplicates
            with engine.begin() as conn:
            # Create the main table if it doesn't exist (copy structure from temp)
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} 
                    (LIKE {temp_table} INCLUDING ALL)
                """)
            
                # Insert new records, avoiding duplicates based on ticker and date
                conn.execute(f"""
                    INSERT INTO {table_name}
                    SELECT t.* FROM {temp_table} t
                    LEFT JOIN {table_name} e
                        ON t.ticker = e.ticker AND t.date = e.date
                    WHERE e.ticker IS NULL
                """)
            
                # Drop the temporary table
                conn.execute(f"DROP TABLE {temp_table}")     

    def load_input(self, context: InputContext):
        engine = create_engine(
            f"postgresql://{self.config.username}:{self.config.password}@"
            f"{self.config.host}:{self.config.port}/{self.config.database}"
        )
        table_name = f"stock.{context.asset_key.path[-1]}"
        return pd.read_sql(f"SELECT * FROM {table_name}", engine)