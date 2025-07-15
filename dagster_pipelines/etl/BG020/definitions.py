from dagster import Definitions, load_assets_from_modules
from dagster_dlt import DagsterDltResource

from dagster_pipelines.etl.BG020 import assets
from dagster_pipelines.etl.BG020.schedules import bg020_daily_job

# one shared Runner resource – works for every dlt pipeline in the project
dlt_resource = DagsterDltResource()

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={"dlt": dlt_resource},
    schedules=[bg020_daily_job],
    asset_checks=[
        assets.check_bg020_file_downloaded,
        assets.check_bg020_dlt_assets,
        assets.check_success_file_upload,
    ]
)