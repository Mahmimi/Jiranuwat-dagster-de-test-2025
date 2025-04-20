import dagster as dg
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb
from datetime import datetime
import duckdb
import pandas as pd
from dagster import asset, Output, MetadataValue
from dagster import get_dagster_logger

logger = get_dagster_logger()

# helper function
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

            logger.info(f"\n== Top 5 rows from {table_name} ==")
            df = con.execute(f"SELECT * FROM plan.plan.{table_name} LIMIT 5").df()

            return Output(
                value=df,
                metadata={"df": MetadataValue.md(df.head().to_markdown())}
            )

    return _preview_asset

# 2.3.1.1 Load pivoted KPI_FY.xlsm into KPI_FY
def validate_data(df: pd.DataFrame, expected_types: dict, passed_info: str) -> pd.DataFrame:
        """
        Validates a DataFrame against a set of expected column types.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            expected_types (dict): A dictionary of column names to their expected data types.
            passed_info (str): A string of information to log if the validation is successful.

        Returns:
            pd.DataFrame: The validated DataFrame.

        Raises:
            ValueError: If a column is missing.
            TypeError: If a column's data type does not match the expected type.
        """
        for col, dtype in expected_types.items():
            if col not in df.columns:
                raise ValueError(f"Missing expected column: {col}")
            if not pd.api.types.is_dtype_equal(df[col].dtype, pd.Series(dtype()).dtype):
                raise TypeError(f"Column '{col}' expected type {dtype}, but got {df[col].dtype}")
        logger.info(passed_info)
        return df

# 1. Read and validate KPI Excel data
@dg.asset(compute_kind="duckdb", group_name="plan")
def read_validated_kpi_fy(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Reads KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file, validates the data types of the columns, and returns the validated DataFrame.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the validated KPI evaluation data.
    """
    ################ to test how error handling works, set validate_dtypes to False ##################
    kpi_data = read_excel(file_path="dagster_pipelines/data/KPI_FY.xlsm", validate_dtypes=True)
    expected_types = {
        "Fiscal_Year": int,
        "Center_ID": str,
        "Kpi Number": str,
        "Kpi_Name": str,
        "Unit": str,
        "Plan_Total": float,
        "Plan_Q1": float,
        "Plan_Q2": float,
        "Plan_Q3": float,
        "Plan_Q4": float,
        "Actual_Total": float,
        "Actual_Q1": float,
        "Actual_Q2": float,
        "Actual_Q3": float,
        "Actual_Q4": float,
    }
    return validate_data(kpi_data, expected_types=expected_types, passed_info="KPI data passed validation.")

# 2. Pivot and validate KPI data
@dg.asset(compute_kind="duckdb", group_name="plan", deps=[read_validated_kpi_fy])
def pivot_validated_kpi_fy(context: dg.AssetExecutionContext, read_validated_kpi_fy: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots and validates the KPI evaluation data using the specified transformation rules.

    This asset performs the following operations:
    - Pivots the KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file.
    - Validates the pivoted DataFrame against a set of expected column types.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.
        read_validated_kpi_fy (pd.DataFrame): A Pandas DataFrame containing the validated KPI evaluation data.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the pivoted and validated KPI data.
    """

    pivot_kpi = pivot_data(read_validated_kpi_fy)
    expected_types = {
        "Fiscal_Year": int,
        "Center_ID": str,
        "Kpi Number": str,
        "Kpi_Name": str,
        "Unit": str,
        "Amount Name": str,
        "Amount": float,
        "Amount Type": str,
    }
    
    return validate_data(pivot_kpi, expected_types=expected_types, passed_info="Pivoted KPI data passed validation.")

# 3. Load final KPI data into DuckDB
@dg.asset(compute_kind="duckdb", group_name="plan", deps=[pivot_validated_kpi_fy])
def kpi_fy(context: dg.AssetExecutionContext, pivot_validated_kpi_fy: pd.DataFrame) -> None:
    """
    Loads the pivoted and validated KPI evaluation data into the "KPI_FY" table in DuckDB.

    This asset performs the following operations:
    - Connects to the DuckDB database.
    - Creates the "KPI_FY" table with a specified schema.
    - Loads the pivoted and validated KPI evaluation data into the "KPI_FY" table.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.
        pivot_validated_kpi_fy (pd.DataFrame): A Pandas DataFrame containing the pivoted and validated KPI evaluation data.
    """
    column_definitions = """
        Fiscal_Year INT,
        Center_ID VARCHAR(8),
        Kpi_Number VARCHAR(6),
        Kpi_Name NVARCHAR,
        Unit NVARCHAR(50),
        Amount_Name VARCHAR(255),
        Amount FLOAT,
        Amount_Type VARCHAR(50)
    """
    load_to_duckdb(pivot_validated_kpi_fy, "KPI_FY", column_definitions)

# preview KPI_FY database
preview_kpi_fy = create_preview_table_asset(
    asset_name="preview_kpi_fy",
    deps=[kpi_fy],
    table_name="KPI_FY"
)

# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan")
def read_validate_m_center(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Reads the center master data from the "M_Center.csv" CSV file and validates the data types of the columns.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the validated center master data.
    """
    center_data = read_csv(file_path="dagster_pipelines/data/M_Center.csv")
    expected_types = {
        "Center_ID": str,
        "Center_Name": str,
    }
    return validate_data(center_data, expected_types=expected_types, passed_info="M_Center data passed validation.")

@dg.asset(compute_kind="duckdb", group_name="plan")
def m_center(context: dg.AssetExecutionContext, read_validate_m_center: pd.DataFrame):

    """
    Loads the validated center master data into the M_Center table in the plan schema.

    Args:
        context (dg.AssetExecutionContext): The execution context for the asset.
        read_validate_m_center (pd.DataFrame): A Pandas DataFrame containing the validated center master data.

    Returns:
        None
    """
    
    column_definitions = "Center_ID VARCHAR(8), Center_Name NVARCHAR"
    load_to_duckdb(read_validate_m_center, "M_Center", column_definitions)

# preview M_Center database
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

        # Join on Center_ID
        df_joined = pd.merge(df_kpi, df_center, on="Center_ID", how="left")

        # Add updated_at column
        df_joined["updated_at"] = datetime.now()

        # Load the joined DataFrame into the DuckDB table
        load_to_duckdb(df_joined, "KPI_FY_Final")

# preview KPI_FY_Final database
preview_kpi_fy_final = create_preview_table_asset(
    asset_name="preview_kpi_fy_final",
    deps=[kpi_fy_final_asset],
    table_name="KPI_FY_Final"
)