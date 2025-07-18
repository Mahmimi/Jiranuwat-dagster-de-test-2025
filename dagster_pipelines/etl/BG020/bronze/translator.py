from typing import Iterator, Dict, Any
from dagster_dlt.translator import DagsterDltTranslator
import dagster as dg
from dagster_dlt.translator import DltResourceTranslatorData


class CustomDagsterDltTranslator(DagsterDltTranslator):
    """Custom translator for Dagster DLT assets to handle specific asset key, dependencies, and description requirements."""

    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        default_spec = super().get_asset_spec(data)
        asset_key_str = "/".join(default_spec.key.path) # Create string of asset keys that match `dg list defs`

        deps = [dg.AssetKey(["bg020", "bronze", "bg020_download_file"])]
        asset_key = dg.AssetKey(["dlt_bg020"]).with_prefix(["bg020", "bronze"])  # Create a new asset key with the prefix "bg020"
        description = f"Asset for DLT pipeline to read .xlsx file and migrate to Microsoft SQL server database with upstream dependencies on bg020_download_file"

        return default_spec.replace_attributes(deps=deps, key=asset_key, description=description)