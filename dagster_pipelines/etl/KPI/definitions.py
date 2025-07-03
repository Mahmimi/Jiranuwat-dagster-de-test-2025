from dagster import Definitions, load_assets_from_modules
from dagster_pipelines.etl.KPI import assets
from dagster_pipelines.etl.KPI.schedules import kpi_fy_monthly_job_schedule

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    schedules=[kpi_fy_monthly_job_schedule],
)