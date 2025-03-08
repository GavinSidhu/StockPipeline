from dagster import IOManager, OutputContext, InputContext
import pandas as pd
from sqlalchemy import create_engine, text
import hashlib

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self.config = config
        
    def handle_output(self, context: OutputContext, obj):
        if isinstance(obj, pd.DataFrame) and not obj.empty:
            # Normalize column names to lowercase to avoid case-sensitivity issues
            obj.columns = [col.lower() for col in obj.columns]
            
            # Get database connection
            engine = create_engine(
                f"postgresql://{self.config.username}:{self.config.password}@"
                f"{self.config.host}:{self.config.port}/{self.config.database}"
            )
            
            # Determine table name
            asset_name = context.asset_key.path[-1]
            schema_name = "stock"
            full_table_name = f"{schema_name}.{asset_name}"
            
            # Create schema if it doesn't exist
            with engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                conn.commit()
            
            # Check if the table exists
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = '{schema_name}' AND table_name = '{asset_name}'
                    )
                """))
                table_exists = result.scalar()
            
            if table_exists:
                # For existing tables, drop and recreate to avoid column mismatch issues
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE {full_table_name}"))
                    conn.commit()
                    
            # Create a fresh table with the current DataFrame schema
            obj.to_sql(
                asset_name, 
                engine, 
                schema=schema_name,
                if_exists="replace", 
                index=False
            )
            
            # Log the successful write
            context.log.info(f"Successfully wrote {len(obj)} rows to {full_table_name}")
            
    def load_input(self, context: InputContext):
        engine = create_engine(
            f"postgresql://{self.config.username}:{self.config.password}@"
            f"{self.config.host}:{self.config.port}/{self.config.database}"
        )
        table_name = f"stock.{context.asset_key.path[-1]}"
        return pd.read_sql(f"SELECT * FROM {table_name}", engine)