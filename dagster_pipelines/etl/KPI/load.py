from dagster import get_dagster_logger
import duckdb
import pandas as pd

logger = get_dagster_logger()

def load_to_duckdb(df: pd.DataFrame, table_name: str, column_definitions: str = None) -> None:
    """
    Load a DataFrame into DuckDB with explicit column types.
    
    Args:
        df (pd.DataFrame): The DataFrame to insert.
        table_name (str): Target table name.
        column_definitions (str): SQL column definitions, e.g., "col1 INT, col2 VARCHAR".
    """
    try:
        with duckdb.connect("/opt/dagster/app/dagster_pipelines/db/plan.db") as con:
            logger.info("Connected to DuckDB successfully.")
            
            con.sql("CREATE SCHEMA IF NOT EXISTS plan;")
            con.register('df_view', df)

            # Create the table with defined schema
            if column_definitions is None:       
                con.sql(f"CREATE OR REPLACE TABLE plan.plan.{table_name} AS SELECT * FROM df_view;")
            else:
                con.sql(f"CREATE OR REPLACE TABLE plan.plan.{table_name} ({column_definitions});")
                
                # Insert from df_view
                con.sql(f"INSERT INTO plan.plan.{table_name} SELECT * FROM df_view;")

            result = con.sql(f"SELECT * FROM plan.plan.{table_name} LIMIT 1").fetchone()
            logger.info(f"Sample record from {table_name}: {result}")
            logger.info(f"Data successfully inserted into table '{table_name}'.")
    except Exception as e:
        logger.error(f"Error loading data into DuckDB: {e}")
        raise
