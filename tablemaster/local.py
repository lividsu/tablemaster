import pandas as pd
import pathlib
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def detect_header_read_csv(path, det_rows=10):
    df = pd.read_csv(path)
    l_unname = len([x for x in df.columns if 'Unnamed' in x])
    if l_unname>1:
        for i in range(det_rows):
            df = pd.read_csv(path, header=i+1)
            if(len([x for x in df.columns if 'Unnamed' in x])==0):
                break
    return df
    
def detect_header_read_excel(path, det_rows=10):
    df = pd.read_excel(path)
    l_unname = len([x for x in df.columns if 'Unnamed' in x])
    if l_unname>1:
        for i in range(det_rows):
            df = pd.read_excel(path, header=i+1)
            if(len([x for x in df.columns if 'Unnamed' in x])==0):
                break
    return df

def equal_table(df1, df2, det_col='nan'):
    if(len(df1) != len(df2)):
        return False
    elif df1.equals(df2):
        return True
    else:
        if det_col == 'nan':
            return False
        else:
            return all(df1[det_col].fillna("").sort_values().reset_index(drop=True).fillna(0) == df2[det_col].fillna("").sort_values().reset_index(drop=True))

def read(file, det_header=True):
    if isinstance(file, pathlib.PosixPath):
        file = str(file)
    file_detect = list(Path().glob(file))
    file_detect = [i for i in file_detect if (str(i)[0]!="." or str(i)[:3]=="../")]
    if len(file_detect)>1:
        raise ValueError(f'There are more than 1 files detected, please specify file name: {file_detect}')
    if len(file_detect) == 0:
        raise FileNotFoundError(f'No file matched: {file}')

    file_path = file_detect[0]
    if file_path.suffix[:3] == '.xl':
        if det_header == True:
            return detect_header_read_excel(file_path)
        return pd.read_excel(file_path)

    if file_path.suffix[:4] == '.csv':
        if det_header == True:
            return detect_header_read_csv(file_path)
        return pd.read_csv(file_path)

    raise Exception(f'unsupported file type: {file_path.suffix}')

def batch_read(file, det_col='nan'):
    path_list = list(Path().glob(file))
    logger.info('below %s files found: %s', len(path_list), path_list)
    dataframes = []
    for i, file in enumerate(path_list):
        df = read(file)
        dataframes.append(df)

    unique_dataframes = []
    for df in dataframes:
        if not any(equal_table(df, existing_df, det_col) for existing_df in unique_dataframes):
            unique_dataframes.append(df)
    logger.info('%s unique files found', len(unique_dataframes))
    return pd.concat(unique_dataframes).reset_index(drop=True)


def read_dfs(file, det_col='nan'):
    path_list = list(Path().glob(file))
    logger.info('below %s files found: %s', len(path_list), path_list)
    dataframes = []
    for i, file in enumerate(path_list):
        df = read(file)
        dataframes.append(df)
    unique_dataframes = []
    for df in dataframes:
        if not any(equal_table(df, existing_df, det_col) for existing_df in unique_dataframes):
            unique_dataframes.append(df)
    logger.info('%s unique files found', len(unique_dataframes))
    return unique_dataframes
