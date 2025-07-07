import dlt

# define the dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="bg020_pipeline",
    destination="mssql",                 
    dataset_name="bg020_demo"
)

