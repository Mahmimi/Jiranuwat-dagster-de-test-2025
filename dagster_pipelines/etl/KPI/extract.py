import pandas as pd

# 2.1.1 Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file
def read_excel(file_path: str = "dagster_pipelines\data\KPI_FY.xlsm", validate_dtypes: bool = True) -> pd.DataFrame:
    """
    Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file. Extract the data and return it as a Pandas DataFrame.

    Args:
        file_path (str): The path to the "KPI_FY.xlsm" Excel file.
        validate_dtypes (bool): If True, validate the data types of the columns.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the KPI evaluation data.
    """

    # Code to read data from the Excel file
    xls = pd.ExcelFile(file_path, engine="openpyxl")
    df = xls.parse("Data to DB")
    if df.empty:
        raise ValueError("No data found in the Excel file.")

    if validate_dtypes:
        # Convert columns with potential mixed types (strings and numbers) to numeric, forcing errors to NaN
        numeric_columns = [
            'Plan_Total', 'Plan_Q1', 'Plan_Q2', 'Plan_Q3', 'Plan_Q4',
            'Actual_Total', 'Actual_Q1', 'Actual_Q2', 'Actual_Q3', 'Actual_Q4'
        ]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Control datatype for other columns
        df = df.astype({
            "Fiscal_Year": int,
            "Center_ID": str,
            "Kpi Number": str,
            "Kpi_Name": str,
            "Unit": str,
        })
        
    return df

# 2.1.2 Read center master data from the "M_Center.csv" CSV file
def read_csv(file_path:str="dagster_pipelines\data\M_Center.csv", validate_dtypes: bool = True) -> pd.DataFrame:
    """
    Read center master data from the "M_Center.csv" CSV file. Extract the data and return it as a Pandas DataFrame.

    Args:
        file_path (str): The path to the "M_Center.csv" CSV file.
        validate_dtypes (bool): If True, validate the data types of the columns.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the center master data.
    """
    
    # Code to read data from the CSV file
    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError("No data found in the CSV file.")

    if validate_dtypes:
        # control datatype
        df = df.astype({
            "Center_ID": str,
            "Center_Name": str,
            })

    return df