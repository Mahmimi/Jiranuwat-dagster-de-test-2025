# dagster_pipelines/definitions.py
from dagster import Definitions
from dagster_dlt import DagsterDltResource

from dagster_pipelines.etl.BG020.assets import bg020_dlt_assets

# one shared Runner resource – works for every dlt pipeline in the project
dlt_resource = DagsterDltResource()

defs = Definitions(
    assets=[bg020_dlt_assets],
    resources={"dlt": dlt_resource},
)
