# bg020_loader.py
import dlt
import pandas as pd
from typing import Iterator, Dict, Any, List

# ────────────────────────────────────────────────────────────────────────────────
# 1.  RESOURCE
#    • Reads the file
#    • Streams it row‑by‑row as dictionaries so dlt can load incrementally
# ────────────────────────────────────────────────────────────────────────────────
@dlt.resource(
    table_name="bg020_loaded_data",     # destination table
    write_disposition="replace",      
    name="BG020_xlsx"                   # resource name in the catalog
)
def read_bg020_excel(file_path: str) -> Iterator[Dict[str, Any]]:
    """Stream rows from the sheet ‘loaded_data’ of an Excel file."""
    xls = pd.ExcelFile(file_path, engine="openpyxl")
    for sheet in xls.sheet_names:                      # loop over all sheets
        df = xls.parse(sheet)                          # read this sheet only
        for row in df.to_dict(orient="records"):       # stream its rows
            row["_sheet"] = sheet                      # optional: tag origin
            yield row


# ────────────────────────────────────────────────────────────────────────────────
# 2.  SOURCE
#    • A thin wrapper that instantiates the resource with a default argument
#    • Can include multiple resources if the file contains many sheets
# ────────────────────────────────────────────────────────────────────────────────
@dlt.source(name="BG020_source")
def bg020_source(file_path: str) -> List[dlt.resource]:
    """A dlt source that exposes the BG020 Excel sheet(s) as resources."""
    return [read_bg020_excel(file_path)]        # instantiate the resource
