import gspread
import json
import math
import pandas as pd
import numpy as np
import re
import warnings
import logging
from datetime import date, datetime, time
from functools import lru_cache

logger = logging.getLogger(__name__)

def _is_google_sheet_id(s):
    return len(s) > 40 and ' ' not in s


def _is_cell_loc(value):
    return isinstance(value, str) and re.match(r'^[A-Za-z]+[1-9]\d*$', value.strip()) is not None


def _warn_deprecated(message):
    warnings.warn(f'{message} This usage will be removed in a future release.', FutureWarning, stacklevel=3)


def _json_safe(value):
    if value is None or value is pd.NA or value is pd.NaT:
        return None

    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, pd.Timedelta):
        return None if pd.isna(value) else str(value)
    if isinstance(value, np.datetime64):
        return None if np.isnat(value) else pd.Timestamp(value).isoformat()
    if isinstance(value, np.timedelta64):
        try:
            if np.isnat(value):
                return None
        except TypeError:
            pass
        return str(pd.Timedelta(value))
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()

    if isinstance(value, (float, np.floating)):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)

    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_json_safe(v) for v in value.tolist()]

    try:
        if math.isnan(value) or math.isinf(value):
            return None
    except Exception:
        pass

    return value


def _build_values(df):
    safe_cols = [_json_safe(col) for col in df.columns.tolist()]
    safe_cols = [("" if col is None else col) for col in safe_cols]
    for col_index, col_value in enumerate(safe_cols, start=1):
        try:
            json.dumps(col_value, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f'column header at col {col_index} is not JSON serializable: {exc}') from exc

    rows = df.values.tolist()
    safe_rows = []
    for row_index, row in enumerate(rows, start=2):
        safe_row = []
        for col_index, cell in enumerate(row, start=1):
            safe_cell = _json_safe(cell)
            safe_cell = '' if safe_cell is None else safe_cell
            try:
                json.dumps(safe_cell, allow_nan=False)
            except (TypeError, ValueError) as exc:
                column_name = str(df.columns[col_index - 1])
                raise ValueError(
                    f'cell at row {row_index}, col {col_index} ({column_name}) is not JSON serializable: {exc}'
                ) from exc
            safe_row.append(safe_cell)
        safe_rows.append(safe_row)

    values = [safe_cols] + safe_rows
    try:
        json.dumps({'values': values}, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'worksheet payload contains non-JSON-safe data: {exc}') from exc
    return values


def _resolve_service_account_path(cfg, service_account_path):
    if service_account_path:
        _warn_deprecated('service_account_path argument is deprecated; pass a cfg object instead.')
        return service_account_path

    if cfg is None:
        _warn_deprecated('No cfg argument provided; use gs_read_df(address, cfg).')
        return None

    if isinstance(cfg, str):
        _warn_deprecated('Passing a string path as the second argument is deprecated; pass a cfg object instead.')
        return cfg

    path = getattr(cfg, 'service_account_path', None)
    if not path:
        raise ValueError('Google config is missing service_account_path; please check cfg.')
    return path


@lru_cache(maxsize=8)
def _get_gspread_client(service_account_path=None):
    if service_account_path:
        return gspread.service_account(service_account_path)
    return gspread.service_account()


def gs_read_df(address, cfg=None, service_account_path=None):
    logger.info('reading google sheets')
    sa_path = _resolve_service_account_path(cfg, service_account_path)
    gc = _get_gspread_client(sa_path)

    spreadsheet_identifier = address[0]
    worksheet_name = address[1]

    try:
        if _is_google_sheet_id(spreadsheet_identifier):
            logger.info('opening sheet by ID: %s', spreadsheet_identifier)
            sh = gc.open_by_key(spreadsheet_identifier)
        else:
            logger.info('opening sheet by name: %s', spreadsheet_identifier)
            sh = gc.open(spreadsheet_identifier)
        
        wks = sh.worksheet(worksheet_name)
        df = pd.DataFrame(wks.get_all_records())
        logger.info('google sheets read success')
        logger.debug('google sheets preview: %s', df.head())
        return df

    except gspread.exceptions.SpreadsheetNotFound:
        message = f"spreadsheet '{spreadsheet_identifier}' not found"
        logger.error(message)
        raise ValueError(message)
    except gspread.exceptions.WorksheetNotFound:
        message = f"worksheet '{worksheet_name}' not found in spreadsheet"
        logger.error(message)
        raise ValueError(message)
    except Exception as e:
        logger.exception('an unexpected error occurred: %s', e)
        raise


def gs_write_df(address, df, cfg=None, loc='A1', service_account_path=None):
    if isinstance(cfg, str) and _is_cell_loc(cfg):
        _warn_deprecated('Passing loc as the third positional argument is deprecated; use keyword loc=...')
        if isinstance(loc, str) and not _is_cell_loc(loc) and service_account_path is None:
            service_account_path = loc
        loc = cfg
        cfg = None

    logger.info('writing google sheets')
    sa_path = _resolve_service_account_path(cfg, service_account_path)
    gc = _get_gspread_client(sa_path)

    spreadsheet_identifier = address[0]
    worksheet_name = address[1]

    is_id = _is_google_sheet_id(spreadsheet_identifier)

    try:
        if is_id:
            logger.info('opening sheet by ID: %s', spreadsheet_identifier)
            sh = gc.open_by_key(spreadsheet_identifier)
        else:
            logger.info('opening sheet by name: %s', spreadsheet_identifier)
            sh = gc.open(spreadsheet_identifier)

    except gspread.exceptions.SpreadsheetNotFound:
        if is_id:
            message = f"spreadsheet ID '{spreadsheet_identifier}' not found, cannot create with specific ID"
            logger.error(message)
            raise ValueError(message)
        else:
            logger.info("spreadsheet '%s' not found, creating one", spreadsheet_identifier)
            sh = gc.create(spreadsheet_identifier)

    try:
        wks = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.info('worksheet "%s" not found, creating one', worksheet_name)
        wks = sh.add_worksheet(title=worksheet_name, rows="100", cols="20")

    try:
        wks.clear()
        df_copy = df.copy()
        values = _build_values(df_copy)
        wks.update(loc, values)

        logger.info('data is written')
    except Exception as e:
        logger.exception('failed to update worksheet: %s', e)
        raise
