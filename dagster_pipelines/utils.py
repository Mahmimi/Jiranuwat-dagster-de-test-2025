from synology_api import filestation
import os, pathlib
from datetime import date
from pathlib import Path
import shutil
import json
import toml
import pyodbc
import re

from dagster import get_dagster_logger

logger = get_dagster_logger()

STATE_FILE = Path("dagster_pipelines/data/bg020_download_state.json")

def save_downloaded_filename(file_key: str, file_name: str):
    """Upserts the downloaded file name under the given file_key in a JSON file.

        Args:
            file_name (str): The name of the downloaded file.
            file_key (str): The key under which the file name will be stored in the JSON file.

        Note:    
            This function creates the directory for the JSON file if it does not exist,
            and initializes the JSON file if it does not exist."""
    
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Load existing state if the file exists
    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            try:
                state = json.load(f)
            except json.JSONDecodeError:
                state = {}
    else:
        state = {}

    # Update or insert the file_key with the new file_name
    state[file_key] = file_name

    # Save back to the file
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def load_downloaded_filename(file_key: str) -> str:
    """ Loads the downloaded file name from the JSON file.
        This is used to retrieve the file name that was downloaded from the NAS.
    """
    if not STATE_FILE.exists():
        raise FileNotFoundError("Downloaded file name not found. Run download step first.")

    with open(STATE_FILE, "r") as f:
        state = json.load(f)
        if file_key not in state:
            raise FileNotFoundError(f"Downloaded file name not found for key: {file_key}")
        return state[file_key]

def clear_downloaded_filename():
    """ Clears the downloaded file name from the JSON file.
        This is used to reset the state after the file has been processed or uploaded.
    """
    if STATE_FILE.exists():
        STATE_FILE.unlink()

class NASfile_handler():
    """ Handles file operations with Synology NAS using the Synology API.
        This class provides methods to download files from a specified NAS folder
        and upload a success file to a designated folder on the NAS.
        It uses the Synology File Station API to interact with the NAS.

        Attributes:
            NAS_IP (str): IP address of the NAS.
            NAS_PORT (str): Port number of the NAS.
            downloaded_file_name (str): Name of the downloaded file. This is set after a successful download via calling download_files_from_nas method.
        
        Methods:
            download_files_from_nas(filename_pattern: str) -> str:
                Downloads the first matching .xlsx file from the NAS folder.
            upload_success_file_nas(file_name: str) -> None:
                Uploads a success file after processing to the NAS with a timestamped name.
                It uses the Synology File Station API to interact with the NAS.
        Usage:
            nas_handler = NASfile_handler()
            nas_handler.download_files_from_nas("BG020")  # Downloads the first matching file
            nas_handler.upload_success_file_nas("BG020.xlsx")  # Uploads a success file with a timestamp
    """

    def __init__(self):
        self.NAS_IP       = "172.30.224.224"
        self.NAS_PORT     = "5000"                 # 5001 + secure=True for HTTPS
        self.__NAS_USERNAME = os.getenv("NAS_USERNAME")
        self.__NAS_PASSWORD = os.getenv("NAS_PASSWORD")
        self.local_dir = Path("dagster_pipelines/data")

        # ---------- 1. connect ----------
        self.__fs = filestation.FileStation(
            ip_address=self.NAS_IP,
            port=self.NAS_PORT,
            username=self.__NAS_USERNAME,
            password=self.__NAS_PASSWORD,
            secure=False,
            cert_verify=False,
            dsm_version=7            # DSM 6 ⇒ 6, DSM 7 ⇒ 7
        )

    def download_files_from_nas(self, filename_pattern:str, file_key: str) -> None:
        """ Downloads the first matching .xlsx file from the NAS folder.
        Downloaded file name will be saved to a .json file for temporary storage.

            Args:
                filename_pattern (str): The pattern to match the file name. 
                                        It should be a prefix of the file name, e.g., "BG020".
                file_key (str): The key under which the downloaded file name will be stored in the JSON file.
            Raises:
                FileNotFoundError: If no matching file is found in the NAS folder.
            Returns:
                None: The method saves the downloaded file name to a JSON file and does not return anything
        """

        # ----------  list the folder ----------
        remote_folder = "/sidataplus-drive/Data Engineer/T_Assets"
        listing = self.__fs.get_file_list(folder_path=remote_folder)        # returns dict
        files   = listing["data"]["files"]                           # list of entries

        # ----------  filter .xlsx files ----------
        xlsx_files = [f for f in files if (not f["isdir"]) and f["name"].lower().endswith(".xlsx") and f["name"].startswith(filename_pattern)]

        # ----------  download ----------
        self.local_dir.mkdir(exist_ok=True)

        f = xlsx_files[0]
        remote_path = f["path"]            # e.g. “…/a.xlsx”
        self.__fs.get_file(
            path   = remote_path,
            mode   = "download",
            dest_path = str(self.local_dir),    #  <─ directory only!
            verify = True
        )

        save_downloaded_filename(file_key, str(f['name']))

        logger.info(f"Downloaded file: {load_downloaded_filename(file_key=file_key)} from NAS to {self.local_dir / f['name']}")

    def upload_success_file_nas(self, file_name: str) -> None:
        """ Uploads a success file after processing to the NAS with a timestamped name.

            Args:
                file_name (str): The name of the file to upload. 
                                 It should be the exact name of the file that was processed, e.g., "BG020.xlsx".
        """
        if not file_name:
            raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
        self.local_dir      = Path("dagster_pipelines/data")
        src            = self.local_dir / file_name
        today_str      = date.today().isoformat()          # 2025-07-04
        renamed_name   = f"{today_str}_{file_name}"
        tmp            = self.local_dir / renamed_name          # temp copy/rename

        shutil.copy2(src, tmp)            # or src.rename(tmp) if you no longer need src

        try:
            self.__fs.upload_file(
                dest_path   = "/sidataplus-drive/Data Engineer/Earth_Onboarding(Success)",
                file_path   = str(tmp),    # basename(tmp) → 2025‑07‑04_a.xlsx
                overwrite   = True
            )
            print(f"✓ uploaded as {renamed_name}")
        finally:
            tmp.unlink(missing_ok=True)    # clean up the temp file

    def check_success_file_exists(self, file_name: str) -> bool:
        """ Checks if the success file exists on the NAS.

            Args:
                file_name (str): The name of the file to check. 
                                 It should be the exact name of the file that was processed, e.g., "BG020.xlsx".
            Returns:
                bool: True if the file exists, False otherwise.
        """
        remote_folder = "/sidataplus-drive/Data Engineer/Earth_Onboarding(Success)"
        listing = self.__fs.get_file_list(folder_path=remote_folder)
        files = listing["data"]["files"]

        return any(f["name"] == file_name for f in files)

class SQLServer_handler():
    """ Handles connection to a Microsoft SQL Server database.
        This class provides methods to connect to the database, execute queries,
        and fetch table statistics such as row count and column count.

        Attributes:
            connection_string (str): The connection string used to connect to the MSSQL database.
        
        Methods:
            connect() -> pyodbc.Connection:
                Establishes a connection to the MSSQL database.
            execute_query(query: str) -> list:
                Executes a SQL query and returns the results.
            get_mssql_credentials_from_dlt() -> str:
                Retrieves the MSSQL credentials from a DLT configuration file.
            fetch_table_stats(table_name: str) -> tuple:
                Fetches the row count and column count for a specified table.
    """
    
    def __init__(self,):
        self.connection_string = self.get_mssql_credentials_from_dlt()
        self.ALLOWED_TABLES ={"bg020_sap_data", "bg020_user_data"}

    def connect(self):
        """ Establishes a connection to the MSSQL database using the connection string.
            Returns:
                pyodbc.Connection: A connection object to the MSSQL database.
        """
        return pyodbc.connect(self.connection_string)
        
    def get_mssql_credentials_from_dlt(self) -> str:
        """ Retrieves the MSSQL credentials from a DLT configuration file.
            Returns:
                str: The connection string for the MSSQL database.
        """
        secrets = toml.load(".dlt/secrets.toml")
        creds = secrets["destination"]["mssql"]["credentials"]
        query_flags = creds.get("query", {})

        driver = creds.get("driver", "ODBC Driver 18 for SQL Server")
        host = creds["host"]
        database = creds["database"]
        username = creds["username"]
        password = creds["password"]
        timeout = creds.get("connect_timeout", 15)

        # Flatten query flags into semicolon-separated string
        query_str = ";".join(f"{k}={v}" for k, v in query_flags.items())

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={host};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Connection Timeout={timeout};"
            f"{query_str};"
        )
        return conn_str

    def fetch_table_stats(self, table_name: str, schema: str = "dbo", database: str = None) -> tuple:
        """
        Fetches the row count and column count for a specified table.

        Args:
            table_name (str): The name of the table to fetch statistics for.
            schema (str): The schema of the table. Defaults to "dbo".
            database (str, optional): The name of the database. If not provided, uses the default database from the connection string.

        Returns:
            tuple: A tuple containing the row count and column count of the table.
        """
        # Validate table_name to avoid SQL injection
        for identifier in [table_name, schema]:
            identifier = identifier.strip()
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
                raise ValueError(f"Invalid identifier: {identifier}")

        full_table = f"[{schema}].[{table_name}]"
        if database:
            full_table = f"[{database}].{full_table}"
        
        if table_name not in self.ALLOWED_TABLES:
            raise ValueError(f"Table {table_name} is not allowed. Allowed tables are: {self.ALLOWED_TABLES}")

        with self.connect() as conn:
            with conn.cursor() as cursor:
                # Row count
                cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
                row_count = cursor.fetchone()[0]

                # Column count
                cursor.execute(f"SELECT TOP 1 * FROM {full_table}")
                columns = [column[0] for column in cursor.description]
                column_count = len(columns)

        return row_count, column_count
