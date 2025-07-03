import dlt

# import sources as bg020_source # your source from earlier

pipeline = dlt.pipeline(
    pipeline_name="bg020_pipeline",
    destination="mssql",                 # just change this
    dataset_name="bg020_demo"
)

# run the load
# info = pipeline.run(bg020_source.bg020_source("dagster_pipelines\\data\\BG020 สัญญาปกติ + consign 62 63 64 65 66 67 V2.XLSX"))
# print(info)              # rows loaded, timings, etc.
