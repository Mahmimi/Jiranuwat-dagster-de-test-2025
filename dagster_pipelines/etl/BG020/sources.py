# bg020_loader.py
import dlt
import pandas as pd
from typing import Iterator, Dict, Any, List
from dagster_pipelines.utils import NASfile_handler

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
    xls = pd.ExcelFile(file_path, engine="openpyxl")
    for sheet in xls.sheet_names:                      
        df = xls.parse(sheet)                          
        for row in df.to_dict(orient="records"):       
            row["_sheet"] = sheet                      
            yield row

# Define the dlt source that uses the read_bg020_excel resource
@dlt.source(name="BG020_source")
def bg020_source(nasfile_handler: NASfile_handler) -> List[dlt.resource]:
    """
    Create a dlt source that reads data from the BG020 Excel file.
    Args:
        nasfile_handler (NASfile_handler): An instance of NASfile_handler to download the file and keep file name to pass to downstream tasks via NASfile_handler.downloaded_file_name.
    Returns:
        List[dlt.resource]: A list containing the dlt resource for reading the BG020 Excel file.
    """
    return [read_bg020_excel(nasfile_handler.download_files_from_nas("BG020"))]
