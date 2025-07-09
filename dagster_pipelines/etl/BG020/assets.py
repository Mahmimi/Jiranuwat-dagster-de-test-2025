# dagster_pipelines/etl/BG020/assets.py
from dagster import AssetExecutionContext, AssetKey
from dagster_dlt import dlt_assets, DagsterDltResource
import dagster as dg
from typing import List
import dlt

from dagster_pipelines.utils import NASfile_handler
import dagster_pipelines.etl.BG020.sources as bg020_source
import dagster_pipelines.etl.BG020.pipelines as pipelines
from dagster_pipelines.etl.BG020.translator import CustomDagsterDltTranslator

# import the NASfile_handler to handle file operations
# This class handles downloading the file from NAS and uploading a success file after processing.
nasfile_handler = NASfile_handler()

@dg.asset(
    key_prefix="bg020",
    group_name="bg020",
    description="Asset that downloads the BG020 Excel file from NAS.",
    name="bg020_download_file",
)
def download_bg020_file() -> str:
    # Use the NASfile_handler to download the file
    file_path = nasfile_handler.download_files_from_nas("BG020")

    return str(file_path)

# Define the Dagster asset group for the dlt pipeline
@dlt_assets(
    dlt_source=bg020_source.bg020_source(download_bg020_file()),
    dlt_pipeline=pipelines.pipeline,
    name="dlt_bg020",
    group_name="bg020",
    dagster_dlt_translator=CustomDagsterDltTranslator(),
)
def bg020_dlt_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    bg020_download_file: str
):
    context.log.info(f"Input file path: {bg020_download_file}")
    yield from dlt.run(context=context)

# Define the asset that uploads a success file to NAS after the dlt pipeline has run
@dg.asset(key_prefix="bg020", 
          deps=[bg020_dlt_assets,], 
          group_name="bg020")
def upload_success_file_to_nas(context: AssetExecutionContext):
    """Uploads a success file to NAS after the dlt pipeline has run."""
    # Ensure the downloaded file name is set
    if not nasfile_handler.downloaded_file_name:
        raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
    else:
        # Call the method to upload the success file
        nasfile_handler.upload_success_file_nas(nasfile_handler.downloaded_file_name)
        # Log the success of the upload operation
        context.log.info(f"Successfully uploaded success file: {nasfile_handler.downloaded_file_name} to NAS.")
        # clear the downloaded file name after upload
        nasfile_handler.downloaded_file_name = None
