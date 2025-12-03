import json
import requests
import pandas as pd


def _get_tenant_access_token(feishu_cfg):
    """获取飞书的 tenant_access_token"""
    feishu_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    post_data = {
        "app_id": feishu_cfg.feishu_app_id,
        "app_secret": feishu_cfg.feishu_app_secret
    }
    r = requests.post(feishu_url, data=post_data)
    return r.json()["tenant_access_token"]


def _col_num_to_letter(n):
    """
    将列号(从1开始)转换为Excel风格的列字母
    例如: 1 -> A, 26 -> Z, 27 -> AA, 28 -> AB
    """
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def fs_read_df(sheet_address, feishu_cfg):
    """
    从飞书电子表格读取数据并返回 DataFrame
    
    Args:
        sheet_address: [spreadsheet_token, sheet_id] 
                      - spreadsheet_token: 表格的唯一标识(URL中sh开头的部分)
                      - sheet_id: 工作表的唯一标识(URL中sheet=后的部分)
        feishu_cfg: 配置对象，包含 feishu_app_id 和 feishu_app_secret
    
    Returns:
        pd.DataFrame: 读取的数据
    """
    tat = _get_tenant_access_token(feishu_cfg)
    header = {
        "content-type": "application/json",
        "Authorization": "Bearer " + str(tat)
    }
    url = (
        "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/" 
        + sheet_address[0] + "/values/" + sheet_address[1]
        + "?valueRenderOption=ToString&dateTimeRenderOption=FormattedString"
    )
    r = requests.get(url, headers=header)
    pull_data = r.json()['data']['valueRange']['values']
    return pd.DataFrame(pull_data[1:], columns=pull_data[0])


def fs_read_base(sheet_address, feishu_cfg):
    """
    从飞书多维表格(Bitable)读取数据并返回 DataFrame
    
    Args:
        sheet_address: [app_token, table_id]
                      - app_token: 多维表格的唯一标识
                      - table_id: 数据表的唯一标识
        feishu_cfg: 配置对象，包含 feishu_app_id 和 feishu_app_secret
    
    Returns:
        pd.DataFrame: 读取的数据
    """
    tat = _get_tenant_access_token(feishu_cfg)
    header = {
        "content-type": "application/json",
        "Authorization": "Bearer " + str(tat)
    }
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/" 
        + sheet_address[0] + "/tables/" + sheet_address[1] + '/records'
        + "?valueRenderOption=ToString&dateTimeRenderOption=FormattedString"
    )
    r = requests.get(url, headers=header)
    pull_data = r.json()['data']['items']
    pull_data_parse = [x['fields'] for x in pull_data]
    return pd.DataFrame(pull_data_parse)


def fs_write_df(sheet_address, df, feishu_cfg, loc='A1', clear_sheet=True):
    """
    将 DataFrame 写入飞书电子表格
    
    Args:
        sheet_address: [spreadsheet_token, sheet_id]
                      - spreadsheet_token: 表格的唯一标识(URL中sh开头的部分)
                      - sheet_id: 工作表的唯一标识(URL中sheet=后的部分)
        df: 要写入的 pandas DataFrame
        feishu_cfg: 配置对象，包含 feishu_app_id 和 feishu_app_secret
        loc: 写入起始位置，默认 'A1'
        clear_sheet: 是否在写入前清空工作表，默认 True
    
    Returns:
        dict: API 响应结果
        
    Example:
        >>> sheet_address = ['shtcnxxxxxx', 'sheet_id_xxx']
        >>> fs_write_df(sheet_address, df, feishu_cfg)
    """
    print('...writing feishu sheets...')
    
    # 获取 access token
    tat = _get_tenant_access_token(feishu_cfg)
    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + str(tat)
    }
    
    spreadsheet_token = sheet_address[0]
    sheet_id = sheet_address[1]
    
    # 清空工作表（如果需要）
    if clear_sheet:
        print(f'...reading existing data to determine clear range...')
        try:
            # 先读取现有数据，获取实际数据范围
            read_url = (
                f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}"
                "?valueRenderOption=ToString&dateTimeRenderOption=FormattedString"
            )
            read_resp = requests.get(read_url, headers=header)
            
            if read_resp.status_code == 200 and read_resp.json().get('code') == 0:
                existing_data = read_resp.json().get('data', {}).get('valueRange', {}).get('values', [])
                
                if existing_data:
                    old_rows = len(existing_data)
                    old_cols = max(len(row) for row in existing_data) if existing_data else 0
                    
                    if old_rows > 0 and old_cols > 0:
                        print(f'...existing data: {old_rows} rows x {old_cols} cols...')
                        
                        # 构建清空范围
                        end_col = _col_num_to_letter(old_cols)
                        clear_range = f"{sheet_id}!A1:{end_col}{old_rows}"
                        
                        print(f'...clearing range: {clear_range}...')
                        
                        # 创建空值矩阵来覆盖
                        empty_values = [[""] * old_cols for _ in range(old_rows)]
                        
                        clear_data = {
                            "valueRange": {
                                "range": clear_range,
                                "values": empty_values
                            }
                        }
                        
                        clear_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
                        clear_resp = requests.put(clear_url, data=json.dumps(clear_data), headers=header)
                        
                        if clear_resp.json().get('code') == 0:
                            print('...sheet cleared!')
                        else:
                            print(f"Warning: Failed to clear sheet: {clear_resp.json().get('msg')}")
                else:
                    print('...sheet is empty, no need to clear...')
            else:
                print(f"Warning: Could not read existing data: {read_resp.json().get('msg', 'Unknown error')}")
                    
        except Exception as e:
            print(f"Warning: Failed to clear sheet: {e}")
    
    # 处理 DataFrame 数据类型
    df_copy = df.copy()
    
    # 将非数值类型转换为字符串
    non_float_int_columns = df_copy.select_dtypes(exclude=['float64', 'int64', 'float32', 'int32']).columns
    for col in non_float_int_columns:
        df_copy[col] = df_copy[col].astype(str)
    
    # 处理 NaN 值，转换为空字符串
    df_copy = df_copy.fillna('')
    
    # 替换 'nan' 字符串为空字符串
    df_copy = df_copy.replace('nan', '')
    df_copy = df_copy.replace('NaT', '')
    
    # 准备写入数据：表头 + 数据
    values = [df_copy.columns.values.tolist()] + df_copy.values.tolist()
    
    # 计算写入范围
    num_rows = len(values)
    num_cols = len(values[0]) if values else 0
    
    # 解析起始位置
    import re
    loc_match = re.match(r'([A-Z]+)(\d+)', loc.upper())
    if loc_match:
        start_col = loc_match.group(1)
        start_row = int(loc_match.group(2))
    else:
        start_col = 'A'
        start_row = 1
    
    # 计算结束列
    start_col_num = sum((ord(c) - ord('A') + 1) * (26 ** i) 
                        for i, c in enumerate(reversed(start_col)))
    end_col_num = start_col_num + num_cols - 1
    end_col = _col_num_to_letter(end_col_num)
    end_row = start_row + num_rows - 1
    
    # 构建写入范围
    write_range = f"{sheet_id}!{start_col}{start_row}:{end_col}{end_row}"
    
    print(f'...writing to range: {write_range}...')
    
    # 构建请求数据
    post_data = {
        "valueRange": {
            "range": write_range,
            "values": values
        }
    }
    
    # 发送 PUT 请求写入数据
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
    
    try:
        r = requests.put(url, data=json.dumps(post_data), headers=header)
        response = r.json()
        
        if response.get('code') == 0:
            print('Data is written!')
        else:
            print(f"Failed to write data: {response.get('msg', 'Unknown error')}")
            print(f"Error code: {response.get('code')}")
        
        return response
        
    except Exception as e:
        print(f"Failed to write data: {e}")
        raise

def _get_bitable_fields(app_token, table_id, header):
    """
    获取多维表格的所有字段名
    
    Returns:
        set: 字段名集合
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    r = requests.get(url, headers=header)
    
    if r.status_code == 200 and r.json().get('code') == 0:
        items = r.json().get('data', {}).get('items', [])
        return {item['field_name'] for item in items}
    else:
        print(f"Warning: Failed to get fields: {r.json().get('msg', 'Unknown error')}")
        return set()

def fs_write_base(sheet_address, df, feishu_cfg, clear_table=False):
    """
    将 DataFrame 写入飞书多维表格(Bitable)
    
    Args:
        sheet_address: [app_token, table_id]
                      - app_token: 多维表格的唯一标识
                      - table_id: 数据表的唯一标识
        df: 要写入的 pandas DataFrame (列名需与多维表格字段名匹配)
        feishu_cfg: 配置对象，包含 feishu_app_id 和 feishu_app_secret
        clear_table: 是否在写入前清空数据表，默认 False
    
    Returns:
        dict: API 响应结果
        
    Note:
        - 多维表格的写入是基于字段名称的，DataFrame的列名需要与表格中的字段名完全匹配
        - 不存在的字段会被自动跳过并打印警告信息
    """
    print('...writing feishu bitable...')
    
    tat = _get_tenant_access_token(feishu_cfg)
    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + str(tat)
    }
    
    app_token = sheet_address[0]
    table_id = sheet_address[1]
    
    # 获取多维表格中的字段名
    print('...fetching bitable fields...')
    existing_fields = _get_bitable_fields(app_token, table_id, header)
    
    if not existing_fields:
        print("Error: Could not fetch table fields or table has no fields!")
        return None
    
    print(f'...table has {len(existing_fields)} fields: {existing_fields}...')
    
    # 检查 DataFrame 列名与表格字段的匹配情况
    df_columns = set(df.columns.tolist())
    
    # 找出不存在的字段
    missing_fields = df_columns - existing_fields
    valid_fields = df_columns & existing_fields
    
    if missing_fields:
        print(f'\n⚠️  WARNING: The following columns do NOT exist in bitable and will be SKIPPED:')
        for field in sorted(missing_fields):
            print(f'    - "{field}"')
        print()
    
    if not valid_fields:
        print("Error: No valid fields to write! All DataFrame columns are missing in bitable.")
        return None
    
    print(f'...will write {len(valid_fields)} valid fields: {valid_fields}...')
    
    # 清空数据表（如果需要）
    if clear_table:
        print('...clearing bitable records...')
        try:
            # 先获取所有记录的 record_id
            list_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
            list_resp = requests.get(list_url, headers=header)
            
            if list_resp.status_code == 200:
                items = list_resp.json().get('data', {}).get('items', [])
                record_ids = [item['record_id'] for item in items]
                
                if record_ids:
                    # 批量删除记录
                    delete_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
                    delete_data = {"records": record_ids}
                    requests.post(delete_url, data=json.dumps(delete_data), headers=header)
                    print(f'...deleted {len(record_ids)} records...')
                    
        except Exception as e:
            print(f"Warning: Failed to clear table: {e}")
    
    # 处理 DataFrame - 只保留有效字段
    df_copy = df[list(valid_fields)].copy()
    df_copy = df_copy.fillna('')
    df_copy = df_copy.replace('nan', '')
    
    # 将 DataFrame 转换为 records 格式
    records = []
    skipped_cols = set()  # 记录因数据类型问题跳过的列
    
    for idx, row in df_copy.iterrows():
        fields = {}
        for col in df_copy.columns:
            value = row[col]
            
            try:
                # 检查是否为空值
                is_na = False
                try:
                    is_na = pd.isna(value)
                    # 如果是数组类型，pd.isna 返回数组，需要用 all() 判断
                    if hasattr(is_na, '__iter__') and not isinstance(is_na, str):
                        is_na = all(is_na) if len(is_na) > 0 else True
                except (ValueError, TypeError):
                    is_na = value is None
                
                if is_na:
                    continue  # 跳过空值
                elif value == '' or value == 'nan' or value == 'None':
                    continue  # 跳过空字符串
                elif isinstance(value, bool):
                    # 布尔值直接写入
                    fields[col] = value
                elif isinstance(value, (int, float)):
                    # 数字直接写入
                    fields[col] = value
                elif isinstance(value, str):
                    # 字符串直接写入
                    fields[col] = value
                elif isinstance(value, (list, tuple)):
                    # 检查列表内容
                    if len(value) == 0:
                        continue
                    first_item = value[0]
                    # 如果是字典列表（如附件、人员字段），提取文本或跳过
                    if isinstance(first_item, dict):
                        # 尝试提取文本内容
                        texts = []
                        for item in value:
                            if isinstance(item, dict):
                                # 尝试获取 text、name、title 等常见文本字段
                                text = item.get('text') or item.get('name') or item.get('title') or item.get('value')
                                if text:
                                    texts.append(str(text))
                        if texts:
                            fields[col] = ', '.join(texts)
                        else:
                            # 无法提取有效文本，跳过该字段
                            if col not in skipped_cols:
                                skipped_cols.add(col)
                            continue
                    else:
                        # 简单列表（如多选字段的字符串列表）
                        fields[col] = [str(item) for item in value]
                elif isinstance(value, dict):
                    # 字典类型，尝试提取文本或转为字符串
                    text = value.get('text') or value.get('name') or value.get('title') or value.get('value')
                    if text:
                        fields[col] = str(text)
                    else:
                        # 转为 JSON 字符串
                        fields[col] = json.dumps(value, ensure_ascii=False)
                else:
                    # 其他类型转为字符串
                    fields[col] = str(value)
                    
            except Exception as e:
                # 如果处理出错，尝试转为字符串
                try:
                    str_val = str(value)
                    if str_val and str_val != 'None' and str_val != 'nan':
                        fields[col] = str_val
                except:
                    if col not in skipped_cols:
                        skipped_cols.add(col)
                    continue
                    
        records.append({"fields": fields})
    
    if skipped_cols:
        print(f'⚠️  WARNING: Skipped columns due to unsupported data types: {skipped_cols}')
    
    # 批量写入（每次最多500条）
    batch_size = 500
    all_responses = []
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        post_data = {"records": batch}
        
        try:
            r = requests.post(url, data=json.dumps(post_data), headers=header)
            response = r.json()
            all_responses.append(response)
            
            if response.get('code') == 0:
                print(f'...batch {i // batch_size + 1}: wrote {len(batch)} records...')
            else:
                print(f"Failed to write batch: {response.get('msg', 'Unknown error')}")
                
        except Exception as e:
            print(f"Failed to write batch: {e}")
    
    # 打印写入总结
    print('\n' + '='*50)
    print('WRITE SUMMARY:')
    print(f'  - Total records: {len(records)}')
    print(f'  - Fields written: {len(valid_fields)}')
    if missing_fields:
        print(f'  - Fields skipped (not in table): {len(missing_fields)}')
        for field in sorted(missing_fields):
            print(f'      × {field}')
    print('='*50)
    print('Data is written!')
    
    return all_responses


# 为了向后兼容，保留原有函数签名的包装
def fs_write_df_simple(map, df, feishu_cfg, loc='A1'):
    """
    简化版写入函数，参数顺序与 gs_write_df 保持一致
    
    Args:
        map: [spreadsheet_token, sheet_id]
        df: pandas DataFrame
        feishu_cfg: 配置对象
        loc: 起始位置，默认 'A1'
    """
    return fs_write_df(map, df, feishu_cfg, loc=loc)