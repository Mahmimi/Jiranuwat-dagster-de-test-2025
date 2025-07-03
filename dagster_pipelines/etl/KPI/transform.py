import pandas as pd

# 2.2.1 Pivot data in the "KPI_FY.xlsm" file
def pivot_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file.

    Args:
        dataframe (pd.DataFrame): A Pandas DataFrame containing the data to pivot. Defualt is the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the pivoted data.
    """

    df = dataframe

    # select columns to pivot
    value_vars = [
        'Plan_Total', 'Plan_Q1', 'Plan_Q2', 'Plan_Q3', 'Plan_Q4',
        'Actual_Total', 'Actual_Q1', 'Actual_Q2', 'Actual_Q3', 'Actual_Q4'
    ]

    # melt the data
    df_melted = df.melt(
        id_vars=['Fiscal_Year', 'Center_ID', 'Kpi Number', 'Kpi_Name', 'Unit'],
        value_vars=value_vars,
        var_name='Amount Name',
        value_name='Amount'
    )

    # add a column for the amount type
    df_melted['Amount Type'] = df_melted['Amount Name'].apply(lambda x: x.split('_')[0])

    return df_melted