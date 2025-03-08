from dagster import IOManager, OutputContext, InputContext
import pandas as pd
from sqlalchemy import create_engine, text
import hashlib

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self.config = config
        
    def handle_output(self, context: OutputContext, obj):
        if isinstance(obj, pd.DataFrame) and not obj.empty:
            # Get database connection
            engine = create_engine(
                f"postgresql://{self.config.username}:{self.config.password}@"
                f"{self.config.host}:{self.config.port}/{self.config.database}"
            )
            
            # Normalize column names to lowercase
            obj.columns = [col.lower() for col in obj.columns]
            
            # Determine table name
            asset_name = context.asset_key.path[-1]
            
            # Create schema if it doesn't exist
            with engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS stock"))
                conn.commit()
            
            # Generate a short hash for the run ID to keep temp table name short
            short_id = hashlib.md5(context.run_id.encode()).hexdigest()[:8]
            temp_table_name = f"temp_{asset_name}_{short_id}"
            
            # Ensure the temp table name is not too long (PostgreSQL limit is 63 chars)
            if len(temp_table_name) > 63:
                temp_table_name = f"temp_{short_id}"
            
            # Write the DataFrame to the temp table
            obj.to_sql(
                temp_table_name, 
                engine, 
                schema="stock",
                if_exists="replace", 
                index=False
            )
            
            # Now handle the main table
            with engine.connect() as conn:
                # Check if the main table exists
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'stock' AND table_name = '{asset_name}'
                    )
                """))
                table_exists = result.scalar()
                
                if not table_exists:
                    # Create the main table from the temp table
                    conn.execute(text(f"""
                        CREATE TABLE stock.{asset_name} AS
                        SELECT * FROM stock.{temp_table_name}
                    """))
                else:
                    # For existing tables, simplify by truncating and reloading
                    # This avoids complex column case matching issues
                    conn.execute(text(f"""
                        TRUNCATE TABLE stock.{asset_name}
                    """))
                    
                    # Copy all data from temp
                    conn.execute(text(f"""
                        INSERT INTO stock.{asset_name}
                        SELECT * FROM stock.{temp_table_name}
                    """))
                
                # Cleanup: Drop the temp table
                conn.execute(text(f"DROP TABLE stock.{temp_table_name}"))
                conn.commit()
            
    def load_input(self, context: InputContext):
        engine = create_engine(
            f"postgresql://{self.config.username}:{self.config.password}@"
            f"{self.config.host}:{self.config.port}/{self.config.database}"
        )
        table_name = f"stock.{context.asset_key.path[-1]}"
        return pd.read_sql(f"SELECT * FROM {table_name}", engine)