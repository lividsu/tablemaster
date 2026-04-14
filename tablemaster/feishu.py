import json
import logging
import time
from datetime import datetime, timedelta

import requests
import pandas as pd

logger = logging.getLogger(__name__)
_TOKEN_CACHE = {}


def _normalize_url(url):
    normalized = url.strip()
    if '`' in normalized:
        logger.warning('url contains backticks, auto sanitize')
        normalized = normalized.replace('`', '').strip()
    return normalized


def _parse_json_response(response, context):
    if response.status_code >= 400:
        body_preview = response.text[:500].replace('\n', ' ')
        raise requests.HTTPError(
            f'{context} failed with status={response.status_code}, body={body_preview}',
            response=response
        )

    content_type = response.headers.get('Content-Type', '')
    if 'application/json' not in content_type.lower():
        body_preview = response.text[:500].replace('\n', ' ')
        raise ValueError(
            f'{context} expected JSON but got Content-Type={content_type}, body={body_preview}'
        )

    try:
        return response.json()
    except ValueError as exc:
        body_preview = response.text[:500].replace('\n', ' ')
        raise ValueError(f'{context} invalid JSON response, body={body_preview}') from exc


def _ensure_feishu_success(body, context):
    if not isinstance(body, dict):
        raise ValueError(f'{context} expected dict JSON body')
    code = body.get('code')
    if code != 0:
        msg = body.get('msg', 'Unknown error')
        raise RuntimeError(f'{context} failed with code={code}, msg={msg}')
    return body


def _request_with_retry(method, url, headers=None, params=None, json_data=None, data=None, timeout=30):
    max_retries = 3
    normalized_url = _normalize_url(url)
    for attempt in range(max_retries):
        response = requests.request(
            method=method,
            url=normalized_url,
            headers=headers,
            params=params,
            json=json_data,
            data=data,
            timeout=timeout,
        )
        if response.status_code != 429:
            return response
        if attempt == max_retries - 1:
            break
        sleep_seconds = 2 ** attempt
        logger.warning('feishu api rate limited (429), retry in %ss', sleep_seconds)
        time.sleep(sleep_seconds)
    raise requests.HTTPError(f'Feishu API rate limited after {max_retries} retries', response=response)


def _get_tenant_access_token(feishu_cfg):
    cache_key = (feishu_cfg.feishu_app_id, feishu_cfg.feishu_app_secret)
    cached = _TOKEN_CACHE.get(cache_key)
    now = datetime.utcnow()
    if cached and now < cached['expire_at'] - timedelta(minutes=5):
        return cached['token']

    feishu_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    post_data = {
        "app_id": feishu_cfg.feishu_app_id,
        "app_secret": feishu_cfg.feishu_app_secret
    }
    r = _request_with_retry("post", feishu_url, json_data=post_data)
    body = _parse_json_response(r, 'get tenant access token')
    _ensure_feishu_success(body, 'get tenant access token')
    token = body.get("tenant_access_token")
    if not token:
        raise KeyError('tenant_access_token is missing in auth response')
    expire_seconds = int(body.get('expire', 7200))
    _TOKEN_CACHE[cache_key] = {
        'token': token,
        'expire_at': now + timedelta(seconds=expire_seconds),
    }
    return token


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
    r = _request_with_retry("get", url, headers=header)
    body = _parse_json_response(r, 'read sheets values')
    _ensure_feishu_success(body, 'read sheets values')

    pull_data = body.get('data', {}).get('valueRange', {}).get('values')
    if not pull_data:
        logger.info('sheet is empty: %s', sheet_address)
        return pd.DataFrame()
    if not isinstance(pull_data, list):
        raise ValueError('unexpected sheets values structure: values is not a list')
    if not pull_data[0]:
        return pd.DataFrame(pull_data[1:] if len(pull_data) > 1 else [])
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
    base_url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        + sheet_address[0] + "/tables/" + sheet_address[1] + '/records'
    )
    pull_data = []
    page_token = None
    has_more = True

    while has_more:
        query_params = "?valueRenderOption=ToString&dateTimeRenderOption=FormattedString&page_size=500"
        if page_token:
            query_params += f"&page_token={page_token}"

        r = _request_with_retry("get", base_url + query_params, headers=header)
        body = _parse_json_response(r, 'read bitable records')
        _ensure_feishu_success(body, 'read bitable records')
        data = body.get('data', {})
        pull_data.extend(data.get('items', []))
        has_more = data.get('has_more', False)
        page_token = data.get('page_token')

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
    logger.info('writing feishu sheets')
    
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
        logger.info('clearing sheet by values_batch_clear api')
        try:
            clear_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_clear"
            clear_data = {"ranges": [f"{sheet_id}!A1:XFD1048576"]}
            clear_resp = _request_with_retry("post", clear_url, headers=header, json_data=clear_data)
            if clear_resp.json().get('code') == 0:
                logger.info('sheet cleared')
            else:
                raise RuntimeError(f"failed to clear sheet: {clear_resp.json().get('msg')}")
        except Exception as e:
            logger.exception('failed to clear sheet: %s', e)
            raise
    
    # 处理 DataFrame 数据类型
    df_copy = df.copy()
    
    # 将非数值类型转换为字符串
    non_float_int_columns = df_copy.select_dtypes(exclude=['float64', 'int64', 'float32', 'int32']).columns
    for col in non_float_int_columns:
        df_copy[col] = df_copy[col].astype(str)
    
    # 处理 NaN 值，转换为空字符串
    df_copy = df_copy.astype(object).fillna('')
    
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
    
    logger.info('writing to range: %s', write_range)
    
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
        r = _request_with_retry("put", url, headers=header, json_data=post_data)
        response = r.json()
        
        if response.get('code') == 0:
            logger.info('data is written')
        else:
            logger.error('failed to write data: %s', response.get('msg', 'Unknown error'))
            logger.error('error code: %s', response.get('code'))
        
        return response
        
    except Exception as e:
        logger.exception('failed to write data: %s', e)
        raise

def _get_bitable_fields(app_token, table_id, header):
    """
    获取多维表格的所有字段名
    
    Returns:
        set: 字段名集合
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    r = _request_with_retry("get", url, headers=header)
    body = _parse_json_response(r, 'get bitable fields')
    _ensure_feishu_success(body, 'get bitable fields')
    items = body.get('data', {}).get('items', [])
    return {item['field_name'] for item in items}

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
    logger.info('writing feishu bitable')
    
    tat = _get_tenant_access_token(feishu_cfg)
    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + str(tat)
    }
    
    app_token = sheet_address[0]
    table_id = sheet_address[1]
    
    # 获取多维表格中的字段名
    logger.info('fetching bitable fields')
    existing_fields = _get_bitable_fields(app_token, table_id, header)
    
    if not existing_fields:
        raise ValueError('could not fetch table fields or table has no fields')
    
    logger.info('table has %s fields', len(existing_fields))
    
    # 检查 DataFrame 列名与表格字段的匹配情况
    df_columns = set(df.columns.tolist())
    
    # 找出不存在的字段
    missing_fields = df_columns - existing_fields
    valid_fields = df_columns & existing_fields
    
    if missing_fields:
        logger.warning('the following columns do not exist in bitable and will be skipped')
        for field in sorted(missing_fields):
            logger.warning('skip column: %s', field)
    
    if not valid_fields:
        raise ValueError('no valid fields to write, all dataframe columns are missing in bitable')
    
    logger.info('will write %s valid fields', len(valid_fields))
    
    # 清空数据表（如果需要）
    if clear_table:
        logger.info('clearing bitable records')
        try:
            list_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
            record_ids = []
            page_token = None
            has_more = True

            while has_more:
                query_params = "?page_size=500"
                if page_token:
                    query_params += f"&page_token={page_token}"
                list_resp = _request_with_retry("get", list_url + query_params, headers=header)

                if list_resp.status_code != 200:
                    break

                data = list_resp.json().get('data', {})
                items = data.get('items', [])
                record_ids.extend([item['record_id'] for item in items])
                has_more = data.get('has_more', False)
                page_token = data.get('page_token')

            if record_ids:
                delete_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
                for i in range(0, len(record_ids), 500):
                    batch_ids = record_ids[i:i + 500]
                    delete_data = {"records": batch_ids}
                    _request_with_retry("post", delete_url, headers=header, json_data=delete_data)
                logger.info('deleted %s records', len(record_ids))

        except Exception as e:
            logger.exception('failed to clear table: %s', e)
            raise
    
    # 处理 DataFrame - 只保留有效字段
    df_copy = df[list(valid_fields)].copy()
    df_copy = df_copy.astype(object).fillna('')
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
                except Exception:
                    if col not in skipped_cols:
                        skipped_cols.add(col)
                    continue
                    
        records.append({"fields": fields})
    
    if skipped_cols:
        logger.warning('skipped columns due to unsupported data types: %s', skipped_cols)
    
    # 批量写入（每次最多500条）
    batch_size = 500
    all_responses = []
    failed_batches = []
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        post_data = {"records": batch}
        
        try:
            r = _request_with_retry("post", url, headers=header, json_data=post_data)
            response = r.json()
            all_responses.append(response)
            
            if response.get('code') == 0:
                logger.info('batch %s wrote %s records', i // batch_size + 1, len(batch))
            else:
                logger.error('failed to write batch: %s', response.get('msg', 'Unknown error'))
                failed_batches.append((i // batch_size + 1, response.get('msg', 'Unknown error')))
                
        except Exception as e:
            logger.exception('failed to write batch: %s', e)
            failed_batches.append((i // batch_size + 1, str(e)))
    
    logger.info('write summary total records: %s', len(records))
    logger.info('write summary fields written: %s', len(valid_fields))
    if missing_fields:
        logger.info('write summary fields skipped: %s', len(missing_fields))
        for field in sorted(missing_fields):
            logger.info('skip field: %s', field)
    if failed_batches:
        raise RuntimeError(f'bitable write failed for {len(failed_batches)} batch(es): {failed_batches}')
    logger.info('data is written')
    
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
