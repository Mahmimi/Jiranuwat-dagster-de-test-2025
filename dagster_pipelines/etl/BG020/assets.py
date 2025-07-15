from dagster import AssetExecutionContext, AssetKey, AssetCheckExecutionContext
from dagster_dlt import dlt_assets, DagsterDltResource
import dagster as dg
from typing import List
import dlt
import pandas as pd
from datetime import date

from dagster_pipelines.utils import NASfile_handler, SQLServer_handler, load_downloaded_filename
import dagster_pipelines.etl.BG020.sources as bg020_source
import dagster_pipelines.etl.BG020.pipelines as pipelines
from dagster_pipelines.etl.BG020.translator import CustomDagsterDltTranslator

# import the NASfile_handler to handle file operations
# This class handles downloading the file from NAS and uploading a success file after processing.
nasfile_handler = NASfile_handler()

# Define the Dagster asset for downloading the BG020 file
@dg.asset(
    key_prefix="bg020",
    group_name="bg020",
    name="bg020_download_file",
)
def download_bg020_file(context: AssetExecutionContext):
    """Downloads the BG020 file from the NAS using the NASfile_handler."""
    # Use the NASfile_handler to download the file
    nasfile_handler.download_files_from_nas("BG020")

# Define the asset check for the downloaded file
@dg.asset_check(asset=download_bg020_file, 
                blocking=True,
                name="check_downloaded_bg020_file")
def check_bg020_file_downloaded(context: AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Check if the BG020 file has been downloaded successfully."""
    try:
        filename = load_downloaded_filename()
        context.log.info(f"Downloaded file: {filename}")
        if not filename:
            raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
        else:
            return dg.AssetCheckResult(passed=True, description=f"File {filename} downloaded successfully.")
        
    except FileNotFoundError as e:
        context.log.error(f"File not found: {e}")
        raise e
    except Exception as e:
        context.log.error(f"An error occurred while loading the downloaded filename: {e}")
        raise e

# Define the Dagster asset group for the dlt pipeline
@dlt_assets(
    dlt_source=bg020_source.bg020_source(),
    dlt_pipeline=pipelines.pipeline,
    name="dlt_bg020",
    group_name="bg020",
    dagster_dlt_translator=CustomDagsterDltTranslator(),
)
def bg020_dlt_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)

# Define the asset check for the dlt pipeline
@dg.asset_check(asset=bg020_dlt_assets, 
                blocking=True,
                name="check_dlt_bg020_assets")
def check_bg020_dlt_assets(context: AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Check if the dlt pipeline has run successfully."""
    try:
        # Ensure the downloaded file name is set
        sql_handler = SQLServer_handler()
        # Fetch table stats to ensure the data has been loaded
        table_name = "[de_dev].[bg020_demo].[bg020_loaded_data]"
        row_count_sql, column_count_sql = sql_handler.fetch_table_stats(table_name)

        # Check if the row count is greater than 0
        if row_count_sql == 0:
            return dg.AssetCheckResult(passed=False, description="No rows found in the loaded data.")
        
        file_path = nasfile_handler.local_dir / load_downloaded_filename()

        with pd.ExcelFile(file_path, engine="openpyxl") as xls:
            df = xls.parse(xls.sheet_names[0])

            row_count_xlsx = len(df)
            column_count_xlsx = len(df.columns)
            # Check if the row count matches the DLT loaded data
            if row_count_sql != row_count_xlsx:
                return dg.AssetCheckResult(passed=False, description=f"Row count mismatch: {row_count_xlsx} in XLSX vs {row_count_sql} in DLT loaded data.")

            # Check if the column count matches the DLT loaded data
            if column_count_sql < column_count_xlsx:
                return dg.AssetCheckResult(passed=False, description=f"Column count mismatch: {column_count_xlsx} in XLSX vs {column_count_sql} in DLT loaded data.")

        # Check data quality
        return dg.AssetCheckResult(passed=True, description="DLT pipeline ran successfully and data quality checks passed. Row and column counts match.")
    except Exception as e:
        context.log.error(f"An error occurred while checking DLT assets: {e}")
        raise e

# Define the asset that uploads a success file to NAS after the dlt pipeline has run
@dg.asset(key_prefix="bg020", 
          deps=[bg020_dlt_assets,], 
          group_name="bg020")
def upload_success_file_to_nas(context: AssetExecutionContext):
    """Uploads a success file to NAS after the dlt pipeline has run."""
    # Ensure the downloaded file name is set
    filename = load_downloaded_filename()
    if not filename:
        raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
    else:
        # Call the method to upload the success file
        nasfile_handler.upload_success_file_nas(filename)
        # Log the success of the upload operation
        context.log.info(f"Successfully uploaded success file: {filename} to NAS.")

# Define asset check for the success file upload
@dg.asset_check(asset=upload_success_file_to_nas, 
                blocking=True,
                name="check_file_uploaded")
def check_success_file_upload(context: AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Check if the success file has been uploaded to NAS."""
    try:
        today_str      = date.today().isoformat()          # 2025-07-04
        filename = str(today_str) + "_" + load_downloaded_filename()
        if not filename:
            raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
        
        # Check if the success file exists in the NAS
        nasfile_handler.check_success_file_exists(filename)
        return dg.AssetCheckResult(passed=True, description=f"Success file {filename} uploaded successfully to NAS.")
    except Exception as e:
        context.log.error(f"An error occurred while checking success file upload: {e}")
        raise e
