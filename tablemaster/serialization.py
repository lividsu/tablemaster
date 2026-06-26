from __future__ import annotations

import json
import math
from datetime import date, datetime, time
from typing import Any

import numpy as np
import pandas as pd


def is_blank(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {'', 'nan', 'nat', 'none'}
    try:
        result = pd.isna(value)
        return bool(result)
    except (TypeError, ValueError):
        return False


def json_safe(value: Any) -> Any:
    if is_blank(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if isinstance(value, np.datetime64):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, np.timedelta64):
        return str(pd.Timedelta(value))
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (float, np.floating)):
        return None if np.isnan(value) or np.isinf(value) else float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return [json_safe(item) for item in value.tolist()]
    try:
        if math.isnan(value) or math.isinf(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def dataframe_to_sheet_values(df: pd.DataFrame) -> list[list[Any]]:
    headers = [json_safe(column) for column in df.columns.tolist()]
    headers = ['' if value is None else value for value in headers]
    rows = [
        ['' if (safe := json_safe(value)) is None else safe for value in row]
        for row in df.itertuples(index=False, name=None)
    ]
    values = [headers, *rows]
    try:
        json.dumps({'values': values}, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'worksheet payload contains non-JSON-safe data: {exc}') from exc
    return values


def _bitable_value(value: Any) -> Any:
    safe = json_safe(value)
    if safe is None:
        return None
    if isinstance(safe, list) and safe and all(isinstance(item, dict) for item in safe):
        texts = [
            item.get('text') or item.get('name') or item.get('title') or item.get('value')
            for item in safe
        ]
        texts = [str(text) for text in texts if text is not None]
        return ', '.join(texts) if texts else None
    if isinstance(safe, dict):
        text = safe.get('text') or safe.get('name') or safe.get('title') or safe.get('value')
        return str(text) if text is not None else json.dumps(safe, ensure_ascii=False)
    return safe


def dataframe_to_bitable_records(
    df: pd.DataFrame,
    valid_fields: set[str],
) -> tuple[list[dict[str, dict[str, Any]]], list[str]]:
    columns = [column for column in df.columns if column in valid_fields]
    skipped = [str(column) for column in df.columns if column not in valid_fields]
    records = []
    for row in df[columns].itertuples(index=False, name=None):
        fields = {}
        for column, value in zip(columns, row):
            rendered = _bitable_value(value)
            if rendered is not None:
                fields[str(column)] = rendered
        records.append({'fields': fields})
    return records, skipped
