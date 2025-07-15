import dlt
import pandas as pd
from typing import Iterator, Dict, Any, List
import os
import zipfile
from pathlib import Path
import time

from dagster_pipelines.utils import NASfile_handler, load_downloaded_filename

# Define the resource for the loaded_data sheet
@dlt.resource(
    table_name="bg020_loaded_data",
    write_disposition="replace",
    name="BG020_xlsx",
)
def read_bg020_excel() -> Iterator[Dict[str, Any]]:
    """
    Stream rows from the sheets of a BG020 Excel file.
    Dynamically mark all empty-value columns as text using dlt.mark.with_hints.
    """
    time.sleep(5)

    local_dir = Path("dagster_pipelines/data")
    file_path = local_dir / Path(load_downloaded_filename())
    size = os.path.getsize(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if not file_path.suffix.lower() == ".xlsx":
        raise ValueError(f"Expected an XLSX file, but got: {file_path.suffix}")
    if size == 0:
        raise RuntimeError(f"{file_path} is empty after download")
    if not zipfile.is_zipfile(file_path):
        raise ValueError(f"Invalid or corrupt XLSX file: {file_path}")

    try:
        with pd.ExcelFile(file_path, engine="openpyxl") as xls:
            df = xls.parse(xls.sheet_names[0])

            # Determine column hints: if entire column is empty, set it to text
            col_hints = {}
            for col in df.columns:
                if df[col].isnull().all():
                    col_hints[col] = {"data_type": "text"}

            for idx, row in enumerate(df.to_dict("records")):
                if idx == 0 and col_hints:
                    yield dlt.mark.with_hints(row, dlt.mark.make_hints(columns=col_hints))
                else:
                    yield row
    except (zipfile.BadZipFile, EOFError, ValueError) as e:
        raise RuntimeError(f"Failed to read Excel file: {e}")

# Define the dlt source that uses the read_bg020_excel resource
@dlt.source(name="BG020_source")
def bg020_source() -> List[dlt.resource]:
    """
    Create a dlt source that reads data from the BG020 Excel file.
    Args:
        file_path (str): Path to the BG020 Excel file.
    Returns:
        List[dlt.resource]: A list containing the dlt resource for reading the BG020 Excel file.
    """

    return [read_bg020_excel, ]  # Return the resource for reading the Excel file
