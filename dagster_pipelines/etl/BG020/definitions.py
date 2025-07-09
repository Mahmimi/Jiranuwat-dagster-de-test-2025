# dagster_pipelines/definitions.py
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
)