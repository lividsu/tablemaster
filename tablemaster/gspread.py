import gspread
import pandas as pd

# 辅助函数，用于判断输入是ID还是名称
def _is_google_sheet_id(s):
    """
    一个简单的启发式方法，用来判断一个字符串是否是 Google Sheet 的ID。
    Google Sheet ID 通常很长（例如44个字符），并且不含空格。
    """
    # 我们可以根据长度和是否包含空格来做一个简单的判断
    return len(s) > 40 and ' ' not in s

def gs_read_df(map, service_account_path=None):
    """
    从 Google Sheet 读取数据到 DataFrame。
    map[0] 可以是表格名称或表格ID。
    """
    print('...reading google sheets...')
    if service_account_path:
        gc = gspread.service_account(service_account_path)
    else:
        gc = gspread.service_account()

    spreadsheet_identifier = map[0]
    worksheet_name = map[1]

    try:
        # 判断是使用ID还是名称打开
        if _is_google_sheet_id(spreadsheet_identifier):
            print(f"...opening sheet by ID: {spreadsheet_identifier}...")
            sh = gc.open_by_key(spreadsheet_identifier)
        else:
            print(f"...opening sheet by name: {spreadsheet_identifier}...")
            sh = gc.open(spreadsheet_identifier)
        
        wks = sh.worksheet(worksheet_name)
        df = pd.DataFrame(wks.get_all_records())
        print('...have read google sheets!...')
        print(df.head())
        return df

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Spreadsheet '{spreadsheet_identifier}' not found.")
        return None # 或者可以抛出异常 raise
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{worksheet_name}' not found in the spreadsheet.")
        return None # 或者可以抛出异常 raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def gs_write_df(map, df, loc='A1', service_account_path=None):
    """
    将 DataFrame 写入 Google Sheet。
    map[0] 可以是表格名称或表格ID。
    """
    print('...writing google sheets...')
    if service_account_path:
        gc = gspread.service_account(service_account_path)
    else:
        gc = gspread.service_account()

    spreadsheet_identifier = map[0]
    worksheet_name = map[1]
    
    # 预先判断是否为ID，这对于后续的错误处理很重要
    is_id = _is_google_sheet_id(spreadsheet_identifier)

    try:
        # 尝试打开表格
        if is_id:
            print(f"...opening sheet by ID: {spreadsheet_identifier}...")
            sh = gc.open_by_key(spreadsheet_identifier)
        else:
            print(f"...opening sheet by name: {spreadsheet_identifier}...")
            sh = gc.open(spreadsheet_identifier)

    except gspread.exceptions.SpreadsheetNotFound:
        # 如果表格未找到，根据是ID还是名称来决定下一步操作
        if is_id:
            # 如果使用ID都找不到，这是一个错误，因为我们无法用指定的ID创建新表
            print(f"Error: Spreadsheet with ID '{spreadsheet_identifier}' not found. Cannot create a sheet with a specific ID.")
            return # 终止函数
        else:
            # 如果是名称找不到，我们可以创建它
            print(f"Spreadsheet '{spreadsheet_identifier}' not found, will create one!")
            sh = gc.create(spreadsheet_identifier)
            # 你可能想在这里分享权限给某些用户
            # sh.share('your-email@example.com', perm_type='user', role='writer')

    try:
        # 尝试打开工作表
        wks = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f'Worksheet "{worksheet_name}" not found, will create one!')
        wks = sh.add_worksheet(title=worksheet_name, rows="100", cols="20")
        
    try:
        # 清空并更新工作表内容
        wks.clear()
        # gspread V6+ 推荐使用 set_dataframe
        # wks.set_dataframe(df, start=loc, copy_head=True) 
        
        # 你的原始方法，处理非数值类型
        non_float_int_columns = df.select_dtypes(exclude=['float64', 'int64']).columns
        for col in non_float_int_columns:
            df[col] = df[col].astype(str)
        wks.update(loc, ([df.columns.values.tolist()] + df.values.tolist()))

        print('Data is written!')
    except Exception as e:
        print(f"Failed to update worksheet: {e}")