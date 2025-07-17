from dagster import AssetExecutionContext, AssetKey, AssetCheckExecutionContext
from dagster_dlt import dlt_assets, DagsterDltResource
import dagster as dg
from typing import List, Generator
import dlt
import pandas as pd
from datetime import date
from time import sleep
import os

from dagster_pipelines.utils import NASfile_handler, SQLServer_handler, load_downloaded_filename, clear_downloaded_filename
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
def download_bg020_file(context: AssetExecutionContext) -> dict:
    """Downloads the BG020 file from the NAS using the NASfile_handler.

    Returns: a dictionary with the local paths of the downloaded files.
    """
    # clear any previously downloaded filenames
    clear_downloaded_filename()

    # file from User
    nasfile_handler.download_files_from_nas("BG020", file_key="user")
    sleep(5)  # Sleep to ensure the file is downloaded before proceeding
    # file from SAP
    nasfile_handler.download_files_from_nas("BG020_ALL_", file_key="sap")

    return { "user": nasfile_handler.local_dir / load_downloaded_filename("user"), "sap": nasfile_handler.local_dir / load_downloaded_filename("sap") }

# Define the asset check for the downloaded file
@dg.multi_asset_check(specs=[
        dg.AssetCheckSpec(name="check_bg020_user_file", asset=download_bg020_file, blocking=True),
        dg.AssetCheckSpec(name="check_bg020_sap_file", asset=download_bg020_file, blocking=True),
    ])
def check_bg020_file_downloaded(context: AssetCheckExecutionContext) -> Generator[dg.AssetCheckResult, None, None]:
    """Check if the BG020 file has been downloaded successfully."""

    def check_file_exists_local(spec_name: str, filename: str) -> dg.AssetCheckResult:
        """Check if the file exists in the NAS folder.
        Args:
            spec_name (str): The name of the asset check specification.
            filename (str): The name of the file to check.
        Returns:
            dg.AssetCheckResult: The result of the asset check.
        """
        if not filename:
            raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
        elif filename in os.listdir(nasfile_handler.local_dir):
            return dg.AssetCheckResult(check_name=spec_name, passed=True, description=f"File {filename} downloaded successfully and exists in the local directory {nasfile_handler.local_dir}.")
        else:
            return dg.AssetCheckResult(check_name=spec_name, passed=False, description=f"File {filename} does not exist in the local directory {nasfile_handler.local_dir}.")

    try:
        yield check_file_exists_local("check_bg020_user_file", load_downloaded_filename("user"))
        yield check_file_exists_local("check_bg020_sap_file", load_downloaded_filename("sap"))
    except Exception as e:
        context.log.error(f"An error occurred while checking downloaded files: {e}")
        raise e
    except FileNotFoundError as e:
        context.log.error(f"File not found: {e}")
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
@dg.multi_asset_check(specs=[
    dg.AssetCheckSpec(name="check_dlt_bg020_user_assets", asset=bg020_dlt_assets, blocking=True),
    dg.AssetCheckSpec(name="check_dlt_bg020_sap_assets", asset=bg020_dlt_assets, blocking=True),
])
def check_bg020_dlt_assets(context: AssetCheckExecutionContext) -> Generator[dg.AssetCheckResult, None, None]:
    """Check if the dlt pipeline has run successfully and the data quality is as expected."""

    def check_dlt_assets(spec_name: str, table_name: str, file_key: str) -> dg.AssetCheckResult:
        """Check if the dlt pipeline has run successfully.
        Args:
            spec_name (str): The name of the asset check specification.
            table_name (str): The name of the table to check.
            file_key (str): The key of the file to check.
        Returns:
            dg.AssetCheckResult: The result of the asset check.
        """
        try:
            # Ensure the downloaded file name is set
            sql_handler = SQLServer_handler()
            # Fetch table stats to ensure the data has been loaded
            row_count_sql, column_count_sql = sql_handler.fetch_table_stats(
                                                                                table_name=table_name,   # just the table name
                                                                                schema="bg020_demo",
                                                                                database="de_dev"
                                                                            )

            # Check if the row count is greater than 0
            if row_count_sql == 0:
                return dg.AssetCheckResult(check_name=spec_name, passed=False, description="No rows found in the loaded data.")

            file_path = nasfile_handler.local_dir / load_downloaded_filename(file_key=file_key)

            with pd.ExcelFile(file_path, engine="openpyxl") as xls:
                df = xls.parse(xls.sheet_names[0])

                row_count_xlsx = len(df)
                column_count_xlsx = len(df.columns)
                # Check if the row count matches the DLT loaded data
                if row_count_sql != row_count_xlsx:
                    return dg.AssetCheckResult(check_name=spec_name, passed=False, description=f"Row count mismatch: {row_count_xlsx} in XLSX vs {row_count_sql} in DLT loaded data.")

                # Check if the column count matches the DLT loaded data
                if column_count_sql < column_count_xlsx:
                    return dg.AssetCheckResult(check_name=spec_name, passed=False, description=f"Column count mismatch: {column_count_xlsx} in XLSX vs {column_count_sql} in DLT loaded data.")

            # Check data quality
            return dg.AssetCheckResult(check_name=spec_name,passed=True, description="DLT pipeline ran successfully and data quality checks passed. Row and column counts match.")
        except Exception as e:
            context.log.error(f"An error occurred while checking DLT assets: {e}")
            raise e
    
    try:
        yield check_dlt_assets("check_dlt_bg020_user_assets", "bg020_user_data", "user")
        yield check_dlt_assets("check_dlt_bg020_sap_assets", "bg020_sap_data", "sap")
    except Exception as e:
        context.log.error(f"An error occurred while checking DLT assets: {e}")
        raise e

# Define the asset that uploads a success file to NAS after the dlt pipeline has run
@dg.asset(key_prefix="bg020", 
        #   ins={"download_bg020_file": dg.AssetIn(key=AssetKey(["bg020", "bg020_download_file"]))}, # alternative way to specify dependencies for list of asset keys and MUST include download_bg020_file as parameter in the function
          deps=[bg020_dlt_assets,], 
          group_name="bg020")
def upload_success_file_to_nas(context: AssetExecutionContext, bg020_download_file: dict) -> None:
    """Uploads a success file to NAS after the dlt pipeline has run.
    Args:
        context (AssetExecutionContext): The context for the asset execution.
        bg020_download_file (dict): The dictionary containing the local paths of the downloaded files.
    """
    def upload_success_file(file_key: str):
        """Uploads a success file to NAS after the dlt pipeline has run.
        Args:
            file_key (str): The key of the file to upload.
        """
        # Ensure the downloaded file name is set
        filename = load_downloaded_filename(file_key=file_key)
        if not filename:
            raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
        else:
            # Call the method to upload the success file
            nasfile_handler.upload_success_file_nas(filename)
            # Log the success of the upload operation
            context.log.info(f"Successfully uploaded success file: {filename} to NAS.")
    
    try:
        context.log.info("data return from download_bg020_file (skip connection dependency): " + str(bg020_download_file)) # dagster allow to skip connection dependency by passing the data directly. this will be used later.
        # Upload success file for user data
        upload_success_file("user")
        # Upload success file for SAP data
        upload_success_file("sap")
    except Exception as e:
        context.log.error(f"An error occurred while uploading success file: {e}")
        raise e
    

# Define asset check for the success file upload
@dg.multi_asset_check(specs=[
    dg.AssetCheckSpec(name="check_user_success_file_upload", asset=upload_success_file_to_nas, blocking=True),
    dg.AssetCheckSpec(name="check_sap_success_file_upload", asset=upload_success_file_to_nas, blocking=True)
])
def check_success_file_upload(context: AssetCheckExecutionContext) -> Generator[dg.AssetCheckResult, None, None]:
    """Check if the success file has been uploaded to NAS after the dlt pipeline has run."""
    def check_file_exists_nas(spec_name: str, file_key: str) -> dg.AssetCheckResult:
        """Check if the success file has been uploaded to NAS.
        Args:
            spec_name (str): The name of the asset check specification.
            file_key (str): The key of the file to check.
        Returns:
            dg.AssetCheckResult: The result of the asset check.
        """
        try:
            today_str      = date.today().isoformat()          # 2025-07-04
            filename = str(today_str) + "_" + load_downloaded_filename(file_key=file_key)
            if not filename:
                raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
            
            # Check if the success file exists in the NAS
            nasfile_handler.check_success_file_exists(filename)
            return dg.AssetCheckResult(check_name=spec_name, passed=True, description=f"Success file {filename} uploaded successfully to NAS.")
        except Exception as e:
            context.log.error(f"An error occurred while checking success file upload: {e}")
            raise e
        
    try:
        yield check_file_exists_nas("check_user_success_file_upload", "user")
        yield check_file_exists_nas("check_sap_success_file_upload", "sap")
    except Exception as e:
        context.log.error(f"An error occurred while checking success file upload: {e}")
        raise e
