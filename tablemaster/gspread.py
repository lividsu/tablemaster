import gspread
import pandas as pd

def gs_read_df(map):
    print('...reading google sheets...')
    gc = gspread.service_account()
    wks = gc.open(map[0]).worksheet(map[1])
    df = pd.DataFrame(wks.get_all_records())
    print('...have read google sheets!...')
    print(df.head())
    return df

def gs_write_df(map, df, loc='A1'):
    print('...writing google sheets...')
    gc = gspread.service_account()
    wks = gc.open(map[0]).worksheet(map[1])
    wks.update(loc,([df.columns.values.tolist()] + df.values.tolist()))
    print('data is written!')