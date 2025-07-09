from typing import Iterator, Dict, Any
from dagster_dlt.translator import DagsterDltTranslator
import dagster as dg
from dagster_dlt.translator import DltResourceTranslatorData


class CustomDagsterDltTranslator(DagsterDltTranslator):

    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        default_spec = super().get_asset_spec(data)
        asset_key_str = "/".join(default_spec.key.path) # Create string of asset keys that match `dg list defs`

        deps = [dg.AssetKey(["bg020_download_file"])]
        asset_key = dg.AssetKey(["dlt_bg020"]).with_prefix("bg020")  # Create a new asset key with the prefix "bg020"

        return default_spec.replace_attributes(deps=deps, key=asset_key) 