import pandas as pd
import pathlib
from pathlib import Path

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

def read(file, det_header=True):
    if isinstance(file, pathlib.PosixPath):
        file = str(file)
    file_detect = list(Path().glob(file))
    file_detect = [i for i in file_detect if str(i)[0]!="."]
    if len(file_detect)>1:
        print(f'There are more 1 file detected, please specify the file name! \n {file_detect}')
        return "error"
    else:
        file_path = file_detect[0]
        if file_path.suffix[:3] == '.xl':
            try:
                if det_header == True:
                    df = detect_header_read_excel(file_path)
                else:
                    df = pd.read_excel(file_path)
            except Exception as e:
                print(e)
        elif file_path.suffix[:4] == '.csv':
            try:
                if det_header == True:
                    df = detect_header_read_csv(file_path)
                else:
                    df = pd.read_csv(file_path)
            except Exception as e:
                print(e)
        else:
            print('unsupported file type!')
        return df

def batch_read(file):
    path_list = list(Path().glob(file))
    print(f'below {len(path_list)} file found: {path_list}')
    dataframes = []
    for i, file in enumerate(path_list):
        df = read(file)
        dataframes.append(df)

    unique_dataframes = []
    for df in dataframes:
        if not any(df.equals(existing_df) for existing_df in unique_dataframes):
            unique_dataframes.append(df)
    print(f'{len(unique_dataframes)}  unique files found!')
    return pd.concat(unique_dataframes).reset_index(drop=True)