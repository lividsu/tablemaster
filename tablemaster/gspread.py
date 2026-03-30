import gspread
import pandas as pd
import re
import warnings
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

def _is_google_sheet_id(s):
    return len(s) > 40 and ' ' not in s


def _is_cell_loc(value):
    return isinstance(value, str) and re.match(r'^[A-Za-z]+[1-9]\d*$', value.strip()) is not None


def _warn_deprecated(message):
    warnings.warn(f'{message} This usage will be removed in a future release.', FutureWarning, stacklevel=3)


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
        logger.error("spreadsheet '%s' not found", spreadsheet_identifier)
        return None
    except gspread.exceptions.WorksheetNotFound:
        logger.error("worksheet '%s' not found in spreadsheet", worksheet_name)
        return None
    except Exception as e:
        logger.exception('an unexpected error occurred: %s', e)
        return None


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
            logger.error("spreadsheet ID '%s' not found, cannot create with specific ID", spreadsheet_identifier)
            return
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
        non_float_int_columns = df_copy.select_dtypes(exclude=['float64', 'int64']).columns
        for col in non_float_int_columns:
            df_copy[col] = df_copy[col].astype(str)
        wks.update(loc, ([df_copy.columns.values.tolist()] + df_copy.values.tolist()))

        logger.info('data is written')
    except Exception as e:
        logger.exception('failed to update worksheet: %s', e)
