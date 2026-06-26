from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests

from .serialization import dataframe_to_bitable_records, dataframe_to_sheet_values

logger = logging.getLogger(__name__)
_TOKEN_CACHE: dict[tuple[str, str], dict[str, Any]] = {}


def _normalize_url(url: str) -> str:
    normalized = url.strip()
    if '`' in normalized:
        logger.warning('URL contains backticks; removing them before the request')
        normalized = normalized.replace('`', '').strip()
    return normalized


def _parse_json_response(response, context: str) -> dict:
    if response.status_code >= 400:
        body = getattr(response, 'text', '')[:500].replace('\n', ' ')
        raise requests.HTTPError(
            f'{context} failed with status={response.status_code}, body={body}',
            response=response,
        )
    try:
        body = response.json()
    except ValueError as exc:
        preview = getattr(response, 'text', '')[:500].replace('\n', ' ')
        raise ValueError(f'{context} returned invalid JSON: {preview}') from exc
    if not isinstance(body, dict):
        raise ValueError(f'{context} expected a JSON object')
    return body


def _ensure_feishu_success(body: dict, context: str) -> dict:
    if body.get('code') != 0:
        raise RuntimeError(
            f'{context} failed with code={body.get("code")}, msg={body.get("msg", "Unknown error")}'
        )
    return body


def _request_with_retry(
    method,
    url,
    headers=None,
    params=None,
    json_data=None,
    data=None,
    timeout=30,
    max_retries=3,
):
    normalized_url = _normalize_url(url)
    response = None
    for attempt in range(max_retries):
        try:
            response = requests.request(
                method=method,
                url=normalized_url,
                headers=headers,
                params=params,
                json=json_data,
                data=data,
                timeout=timeout,
            )
        except requests.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(2**attempt)
            continue
        if response.status_code != 429 and response.status_code < 500:
            return response
        if attempt < max_retries - 1:
            delay = 2**attempt
            logger.warning('Feishu API returned %s; retrying in %ss', response.status_code, delay)
            time.sleep(delay)
    raise requests.HTTPError(
        f'Feishu API failed after {max_retries} attempts',
        response=response,
    )


def _auth_headers(feishu_cfg) -> dict[str, str]:
    return {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {_get_tenant_access_token(feishu_cfg)}',
    }


def _get_tenant_access_token(feishu_cfg):
    cache_key = (feishu_cfg.feishu_app_id, feishu_cfg.feishu_app_secret)
    now = datetime.now(timezone.utc)
    cached = _TOKEN_CACHE.get(cache_key)
    if cached and now < cached['expire_at'] - timedelta(minutes=5):
        return cached['token']

    response = _request_with_retry(
        'post',
        'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/',
        json_data={
            'app_id': feishu_cfg.feishu_app_id,
            'app_secret': feishu_cfg.feishu_app_secret,
        },
    )
    body = _ensure_feishu_success(_parse_json_response(response, 'get tenant access token'), 'get tenant access token')
    token = body.get('tenant_access_token')
    if not token:
        raise ValueError('Feishu token response did not include tenant_access_token')
    expire_seconds = int(body.get('expire', 7200))
    _TOKEN_CACHE[cache_key] = {
        'token': token,
        'expire_at': now + timedelta(seconds=expire_seconds),
    }
    return token


def _column_number_to_letters(number: int) -> str:
    if number < 1:
        raise ValueError('column number must be positive')
    letters = ''
    while number:
        number, remainder = divmod(number - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _parse_start_cell(loc: str) -> tuple[str, int, int]:
    match = re.fullmatch(r'([A-Za-z]+)([1-9]\d*)', loc.strip())
    if not match:
        raise ValueError(f'Invalid cell location: {loc!r}')
    letters = match.group(1).upper()
    column = 0
    for char in letters:
        column = column * 26 + ord(char) - 64
    return letters, int(match.group(2)), column


def fs_read_df(sheet_address, feishu_cfg) -> pd.DataFrame:
    spreadsheet_token, sheet_id = sheet_address
    url = (
        f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/'
        f'{spreadsheet_token}/values/{sheet_id}'
    )
    response = _request_with_retry(
        'get',
        url,
        headers=_auth_headers(feishu_cfg),
        params={
            'valueRenderOption': 'ToString',
            'dateTimeRenderOption': 'FormattedString',
        },
    )
    body = _ensure_feishu_success(_parse_json_response(response, 'read sheet values'), 'read sheet values')
    values = body.get('data', {}).get('valueRange', {}).get('values') or []
    if not values or not values[0]:
        return pd.DataFrame()
    return pd.DataFrame(values[1:], columns=values[0])


def fs_read_base(sheet_address, feishu_cfg) -> pd.DataFrame:
    app_token, table_id = sheet_address
    url = f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records'
    items = []
    page_token = None
    while True:
        params = {'page_size': 500}
        if page_token:
            params['page_token'] = page_token
        response = _request_with_retry('get', url, headers=_auth_headers(feishu_cfg), params=params)
        body = _ensure_feishu_success(_parse_json_response(response, 'read bitable records'), 'read bitable records')
        data = body.get('data', {})
        items.extend(data.get('items') or [])
        if not data.get('has_more'):
            break
        page_token = data.get('page_token')
        if not page_token:
            raise ValueError('Feishu pagination indicated more records without a page token')
    return pd.DataFrame([item.get('fields', {}) for item in items])


def _clear_sheet(spreadsheet_token: str, sheet_id: str, headers: dict[str, str]) -> None:
    urls = [
        f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_clear',
        f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/batch_clear',
    ]
    for url in urls:
        response = _request_with_retry(
            'post',
            url,
            headers=headers,
            json_data={'ranges': [f'{sheet_id}!A1:XFD1048576']},
        )
        if response.status_code == 404:
            continue
        body = _parse_json_response(response, 'clear sheet')
        _ensure_feishu_success(body, 'clear sheet')
        return
    raise RuntimeError('No supported Feishu sheet-clear endpoint was available')


def fs_write_df(sheet_address, df, feishu_cfg, loc='A1', clear_sheet=True):
    spreadsheet_token, sheet_id = sheet_address
    if len(df.columns) == 0:
        raise ValueError('Cannot write a DataFrame with no columns')
    headers = _auth_headers(feishu_cfg)
    if clear_sheet:
        _clear_sheet(spreadsheet_token, sheet_id, headers)

    start_letters, start_row, start_column = _parse_start_cell(loc)
    values = dataframe_to_sheet_values(df)
    end_column = _column_number_to_letters(start_column + len(df.columns) - 1)
    end_row = start_row + len(values) - 1
    payload = {
        'valueRange': {
            'range': f'{sheet_id}!{start_letters}{start_row}:{end_column}{end_row}',
            'values': values,
        }
    }
    response = _request_with_retry(
        'put',
        f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values',
        headers=headers,
        json_data=payload,
    )
    body = _parse_json_response(response, 'write sheet values')
    return _ensure_feishu_success(body, 'write sheet values')


def _get_bitable_fields(app_token, table_id, header) -> set[str]:
    url = f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields'
    fields: set[str] = set()
    page_token = None
    while True:
        params = {'page_size': 100}
        if page_token:
            params['page_token'] = page_token
        response = _request_with_retry('get', url, headers=header, params=params)
        body = _ensure_feishu_success(_parse_json_response(response, 'get bitable fields'), 'get bitable fields')
        data = body.get('data', {})
        fields.update(
            item['field_name']
            for item in data.get('items') or []
            if item.get('field_name')
        )
        if not data.get('has_more'):
            return fields
        page_token = data.get('page_token')
        if not page_token:
            raise ValueError('Feishu field pagination indicated more data without a page token')


def _list_bitable_record_ids(app_token, table_id, headers) -> list[str]:
    url = f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records'
    record_ids = []
    page_token = None
    while True:
        params = {'page_size': 500}
        if page_token:
            params['page_token'] = page_token
        response = _request_with_retry('get', url, headers=headers, params=params)
        body = _ensure_feishu_success(_parse_json_response(response, 'list bitable records'), 'list bitable records')
        data = body.get('data', {})
        record_ids.extend(item['record_id'] for item in data.get('items') or [])
        if not data.get('has_more'):
            return record_ids
        page_token = data.get('page_token')
        if not page_token:
            raise ValueError('Feishu record pagination indicated more data without a page token')


def _clear_bitable(app_token, table_id, headers) -> None:
    record_ids = _list_bitable_record_ids(app_token, table_id, headers)
    url = (
        f'https://open.feishu.cn/open-apis/bitable/v1/apps/'
        f'{app_token}/tables/{table_id}/records/batch_delete'
    )
    for start in range(0, len(record_ids), 500):
        response = _request_with_retry(
            'post',
            url,
            headers=headers,
            json_data={'records': record_ids[start : start + 500]},
        )
        body = _parse_json_response(response, 'delete bitable records')
        _ensure_feishu_success(body, 'delete bitable records')


def fs_write_base(sheet_address, df, feishu_cfg, clear_table=False):
    app_token, table_id = sheet_address
    headers = _auth_headers(feishu_cfg)
    fields = _get_bitable_fields(app_token, table_id, headers)
    if not fields:
        raise ValueError('Bitable has no writable fields')
    if clear_table:
        _clear_bitable(app_token, table_id, headers)

    records, skipped_columns = dataframe_to_bitable_records(df, fields)
    if skipped_columns:
        logger.warning('Skipping columns absent from Bitable: %s', ', '.join(skipped_columns))
    if not any(column in fields for column in df.columns):
        raise ValueError('No DataFrame columns match Bitable fields')

    url = (
        f'https://open.feishu.cn/open-apis/bitable/v1/apps/'
        f'{app_token}/tables/{table_id}/records/batch_create'
    )
    responses = []
    failures = []
    for start in range(0, len(records), 500):
        try:
            response = _request_with_retry(
                'post',
                url,
                headers=headers,
                json_data={'records': records[start : start + 500]},
            )
            body = _parse_json_response(response, 'write bitable records')
            _ensure_feishu_success(body, 'write bitable records')
            responses.append(body)
        except Exception as exc:
            failures.append((start // 500 + 1, str(exc)))
    if failures:
        raise RuntimeError(f'bitable write failed for {len(failures)} batch(es): {failures}')
    return responses


def fs_write_df_simple(map, df, feishu_cfg, loc='A1'):
    return fs_write_df(map, df, feishu_cfg, loc=loc)
