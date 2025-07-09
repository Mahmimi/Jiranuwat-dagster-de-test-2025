# bg020_loader.py
import dlt
import pandas as pd
from typing import Iterator, Dict, Any, List
import os
import zipfile
from pathlib import Path
import time

# Define the resource for the loaded_data sheet
@dlt.resource(
    table_name="bg020_loaded_data",     
    write_disposition="replace",      
    name="BG020_xlsx"                   
)
def read_bg020_excel(file_path: str) -> Iterator[Dict[str, Any]]:
    """
    Stream rows from the sheets of a BG020 Excel file.
    Each row is yielded as a dictionary with an additional key '_sheet'
    indicating the sheet name.
    
    Args:
        file_path (str): Path to the Excel file.
    Yields:
        Dict[str, Any]: A dictionary representing each row in the Excel file,
        with an additional key '_sheet' for the sheet name.
    """

    time.sleep(5)

    file_path = Path(file_path)

    size = os.path.getsize(file_path)
    if size == 0:
        raise RuntimeError(f"{file_path} is empty after download")
    if not zipfile.is_zipfile(file_path):
        raise ValueError(f"Invalid or corrupt XLSX file: {file_path}")

    try:
        with pd.ExcelFile(file_path, engine="openpyxl") as xls:
            df = xls.parse(xls.sheet_names[0])
            for row in df.to_dict("records"):
                yield row
    except (zipfile.BadZipFile, EOFError, ValueError) as e:
        raise RuntimeError(f"Failed to read Excel file: {e}")

# Define the dlt source that uses the read_bg020_excel resource
@dlt.source(name="BG020_source")
def bg020_source(file_path: str) -> List[dlt.resource]:
    """
    Create a dlt source that reads data from the BG020 Excel file.
    Args:
        file_path (str): Path to the BG020 Excel file.
    Returns:
        List[dlt.resource]: A list containing the dlt resource for reading the BG020 Excel file.
    """

    return [read_bg020_excel(file_path=file_path), ]  # Return the resource for reading the Excel file
