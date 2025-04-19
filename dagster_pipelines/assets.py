import dagster as dg
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb
from datetime import datetime
import duckdb
import pandas as pd
from dagster import asset, Output, MetadataValue
from dagster import get_dagster_logger

def create_preview_table_asset(asset_name: str, deps: list, table_name: str):
    """
    Creates a Dagster asset that previews the top 5 rows from a specified DuckDB table.

    This Decorator function dynamically generates an asset function that connects to the DuckDB database,
    retrieves the top 5 rows from the specified table, and returns the result as a Pandas DataFrame
    wrapped in a Dagster Output object. The output includes metadata containing a markdown
    representation of the DataFrame.

    Args:
        asset_name (str): The name of the asset to be created.
        deps (list): The dependencies for this asset.
        table_name (str): The name of the DuckDB table to preview.
    
    Returns:
        Callable: A function that, when executed, retrieves and returns the preview of the table.
    """

    @asset(name=asset_name, compute_kind="duckdb", group_name="plan", deps=deps)
    def _preview_asset() -> Output[pd.DataFrame]:
        with duckdb.connect("/opt/dagster/app/dagster_pipelines/db/plan.db") as con:
            logger = get_dagster_logger()

            logger.info(f"\n== Top 5 rows from {table_name} ==")
            df = con.execute(f"SELECT * FROM plan.plan.{table_name} LIMIT 5").df()

            con.close()

            return Output(
                value=df,
                metadata={"df": MetadataValue.md(df.head().to_markdown())}
            )

    return _preview_asset

# 2.3.1.1 Load pivoted KPI_FY.xlsm into KPI_FY
@dg.asset(compute_kind="duckdb", group_name="plan")
def kpi_fy(context: dg.AssetExecutionContext):
    """
    Load and transform KPI evaluation data into the KPI_FY table in DuckDB.

    This asset performs the following operations:
    - Reads KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file.
    - Pivots the data to reshape it for analysis.
    - Loads the transformed data into the 'KPI_FY' table within the DuckDB database in the 'plan' schema.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.
    """

    kpi_data = read_excel(file_path="dagster_pipelines/data/KPI_FY.xlsm")
    pivot_kpi = pivot_data(kpi_data)
    load_to_duckdb(pivot_kpi, "KPI_FY")

# Define your asset instance with custom params
preview_kpi_fy = create_preview_table_asset(
    asset_name="preview_kpi_fy",
    deps=[kpi_fy],
    table_name="KPI_FY"
)

# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan")
def m_center(context: dg.AssetExecutionContext):
    """
    Load the center master data from "M_Center.csv" into the M_Center table in DuckDB.

    This asset performs the following operations:
    - Reads the center master data from the specified CSV file.
    - Loads the data into the 'M_Center' table in the DuckDB database within the 'plan' schema.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.
    """

    center_data = read_csv(file_path="dagster_pipelines/data/M_Center.csv")
    load_to_duckdb(center_data, "M_Center")

preview_m_center = create_preview_table_asset(
    asset_name="preview_m_center",
    deps=[m_center],
    table_name="M_Center"
)

# 2.3.2 Create asset kpi_fy_final_asset()
@dg.asset(compute_kind="duckdb", group_name="plan", deps=[preview_kpi_fy, preview_m_center])
def kpi_fy_final_asset(context: dg.AssetExecutionContext):

    """
    Joins the KPI_FY and M_Center tables in DuckDB based on Center_ID, adds an updated_at column, and loads the joined DataFrame into the KPI_FY_Final table in the plan schema.

    This asset performs the following operations:
    - Connects to the DuckDB database.
    - Queries the KPI_FY and M_Center tables.
    - Joins the two DataFrames on Center_ID using a left join.
    - Adds an updated_at column with the current datetime.
    - Loads the joined DataFrame into the KPI_FY_Final table in the plan schema.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.
    """
    
    # Connect to the DuckDB database
    with duckdb.connect("/opt/dagster/app/dagster_pipelines/db/plan.db") as con:

        # Query both tables
        df_kpi = con.execute("SELECT * FROM plan.plan.KPI_FY").fetchdf()
        df_center = con.execute("SELECT * FROM plan.plan.M_Center").fetchdf()
        con.close()

        # Join on Center_ID
        df_joined = pd.merge(df_kpi, df_center, on="Center_ID", how="left")

        # Add updated_at column
        df_joined["updated_at"] = datetime.now()

        # Load the joined DataFrame into the DuckDB table
        load_to_duckdb(df_joined, "KPI_FY_Final")

preview_kpi_fy_final = create_preview_table_asset(
    asset_name="preview_kpi_fy_final",
    deps=[kpi_fy_final_asset],
    table_name="KPI_FY_Final"
)