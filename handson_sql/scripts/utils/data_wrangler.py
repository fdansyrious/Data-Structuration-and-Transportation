from typing import Optional
import logging
import pandas as pd
from env import TABLE_DATA_MAPPING_PATH


def read_datas(table_file_mapping:dict = TABLE_DATA_MAPPING_PATH) -> dict[str, pd.DataFrame]:
    """
    Reads data associated with the tables
    
    Parameters:
    TABLE_DATA_MAPPING_PATH : dictionary mapping tables to their source file
    """
    datas = dict()
    for table_name, file in TABLE_DATA_MAPPING_PATH.items():
        datas[table_name] = pd.read_csv(file)
    return datas

def wrangler_data_for_insertion(df):
    columns = df.columns.tolist()
    columns = f"({", ".join(columns)})"
    
    values = "("+df.astype(str).apply(lambda cols:", ".join([f'"{value}"' for value in cols]), axis=1)+")"
    values = values.values.tolist()
    values = ",\n".join(values)
    return columns, values