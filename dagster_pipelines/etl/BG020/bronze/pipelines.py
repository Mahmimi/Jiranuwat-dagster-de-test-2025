import dlt
from dagster_pipelines.etl.BG020.bronze.sources import bg020_source

# define the dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="bg020_pipeline",
    destination="mssql",                 
    dataset_name="bg020_demo"
)
