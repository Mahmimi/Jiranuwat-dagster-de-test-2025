from synology_api import filestation
import os, pathlib
from datetime import date
from pathlib import Path
import shutil
import json        

from dagster import get_dagster_logger

logger = get_dagster_logger()

STATE_FILE = Path("dagster_pipelines/data/bg020_download_state.json")

def save_downloaded_filename(file_name: str):
    """ Saves the downloaded file name to a JSON file for temporary storage.
        This is used to keep track of the file that was downloaded from the NAS.
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"file_name": file_name}, f)

def load_downloaded_filename() -> str:
    """ Loads the downloaded file name from the JSON file.
        This is used to retrieve the file name that was downloaded from the NAS.
    """
    if not STATE_FILE.exists():
        raise FileNotFoundError("Downloaded file name not found. Run download step first.")
    with open(STATE_FILE, "r") as f:
        return json.load(f)["file_name"]

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

    def download_files_from_nas(self, filename_pattern:str) -> None:
        """ Downloads the first matching .xlsx file from the NAS folder.
        Downloaded file name will be saved to a .json file for temporary storage.

            Args:
                filename_pattern (str): The pattern to match the file name. 
                                        It should be a prefix of the file name, e.g., "BG020".
        """

        # ----------  list the folder ----------
        remote_folder = "/sidataplus-drive/Data Engineer/T_Assets"
        listing = self.__fs.get_file_list(folder_path=remote_folder)        # returns dict
        files   = listing["data"]["files"]                           # list of entries

        # ----------  filter .xlsx files ----------
        xlsx_files = [f for f in files if (not f["isdir"]) and f["name"].lower().endswith(".xlsx") and f["name"].startswith(filename_pattern)]

        # ----------  download ----------
        local_dir = pathlib.Path("dagster_pipelines/data")
        local_dir.mkdir(exist_ok=True)

        f = xlsx_files[0]
        remote_path = f["path"]            # e.g. “…/a.xlsx”
        self.__fs.get_file(
            path   = remote_path,
            mode   = "download",
            dest_path = str(local_dir),    #  <─ directory only!
            verify = True
        )

        save_downloaded_filename(str(f['name']))

        logger.info(f"Downloaded file: {load_downloaded_filename()} from NAS to {local_dir / f['name']}")
        
    def upload_success_file_nas(self, file_name: str) -> None:
        """ Uploads a success file after processing to the NAS with a timestamped name.

            Args:
                file_name (str): The name of the file to upload. 
                                 It should be the exact name of the file that was processed, e.g., "BG020.xlsx".
        """
        if not file_name:
            raise ValueError("No file has been downloaded from NAS. Please run the download step first.")
        local_dir      = Path("dagster_pipelines/data")
        src            = local_dir / file_name
        today_str      = date.today().isoformat()          # 2025-07-04
        renamed_name   = f"{today_str}_{file_name}"
        tmp            = local_dir / renamed_name          # temp copy/rename

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
