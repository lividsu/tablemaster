import gspread
import pandas as pd

def gs_read_df(map,service_account_path=None):
    print('...reading google sheets...')
    if service_account_path:
        gc = gspread.service_account(service_account_path)
    else:
        gc = gspread.service_account()
    wks = gc.open(map[0]).worksheet(map[1])
    df = pd.DataFrame(wks.get_all_records())
    print('...have read google sheets!...')
    print(df.head())
    return df

def gs_write_df(map, df, loc='A1',service_account_path=None):
    print('...writing google sheets...')
    if service_account_path:
        gc = gspread.service_account(service_account_path)
    else:
        gc = gspread.service_account()
    try:
        # Try to open the spreadsheet
        sh = gc.open(map[0])
    except gspread.exceptions.SpreadsheetNotFound:
        print(f'Spreadsheet {map[0]} not found, will create one!')
        sh = gc.create(map[0])
    try:
        # Try to open the worksheet
        wks = sh.worksheet(map[1])
    except gspread.exceptions.WorksheetNotFound:
        print(f'Worksheet {map[1]} not found, will create one!')
        wks = sh.add_worksheet(title=map[1], rows="100", cols="20")
    try:
        # Clear the worksheet and update with new data
        wks.clear()
        non_float_int_columns = df.select_dtypes(exclude=['float64', 'int64']).columns
        for col in non_float_int_columns:
            df[col] = df[col].astype(str)
        wks.update(loc, ([df.columns.values.tolist()] + df.values.tolist()))
        print('Data is written!')
    except Exception as e:
        print(f"Failed to update worksheet: {e}")
