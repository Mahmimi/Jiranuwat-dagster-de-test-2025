# dagster_pipelines/etl/BG020/assets.py
from dagster import AssetExecutionContext, AssetKey
from dagster_dlt import dlt_assets, DagsterDltResource
import dagster as dg

from dagster_pipelines.utils import NASfile_handler
import dagster_pipelines.etl.BG020.sources as bg020_source
import dagster_pipelines.etl.BG020.pipelines as pipelines

# import the NASfile_handler to handle file operations
# This class handles downloading the file from NAS and uploading a success file after processing.
nasfile_handler = NASfile_handler()

# Define the Dagster asset group for the dlt pipeline
@dlt_assets(
    dlt_source=bg020_source.bg020_source(nasfile_handler),
    dlt_pipeline=pipelines.pipeline,
    name="bg020",            
    group_name="bg020",
)
def bg020_dlt_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,  
):
    """Runs the dlt pipeline and yields Dagster materializations."""
    yield from dlt.run(context=context)

# Define the asset that uploads a success file to NAS after the dlt pipeline has run
@dg.asset(deps=[bg020_dlt_assets,])
def upload_success_file_to_nas(context: AssetExecutionContext):
    """Uploads a success file to NAS after the dlt pipeline has run."""
    # Ensure the downloaded file name is set
    if not nasfile_handler.downloaded_file_name:
        raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
    # Call the method to upload the success file
    nasfile_handler.upload_success_file_nas(nasfile_handler.downloaded_file_name)
    # Upload the success file to NAS
    context.log.info(f"Uploading success file: {nasfile_handler.downloaded_file_name} to NAS.")
