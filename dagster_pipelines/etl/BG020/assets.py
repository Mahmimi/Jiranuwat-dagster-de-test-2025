# dagster_pipelines/etl/BG020/assets.py
from dagster import AssetExecutionContext
from dagster_dlt import dlt_assets, DagsterDltResource
import dlt

import dagster_pipelines.etl.BG020.sources as bg020_source
import dagster_pipelines.etl.BG020.pipelines as pipelines

FILE_PATH = "dagster_pipelines/data/BG020 สัญญาปกติ + consign 62 63 64 65 66 67 V2.XLSX"

# ---------- multi‑asset ----------------------------------------------------
@dlt_assets(
    # this builds the DltSource object right now
    dlt_source=bg020_source.bg020_source(FILE_PATH),
    dlt_pipeline=pipelines.pipeline,
    name="bg020",            # Dagster asset group name
    group_name="bg020",
)
def bg020_dlt_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,  # injected resource
):
    """Runs the dlt pipeline and yields Dagster materializations."""
    yield from dlt.run(context=context)
