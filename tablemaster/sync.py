import logging
import re
from collections import OrderedDict

import pandas as pd

from .database import ManageTable, query
from .feishu import fs_read_df, fs_write_df

logger = logging.getLogger(__name__)


def _safe_identifier(identifier):
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', identifier):
        raise ValueError(f'Invalid identifier: {identifier}')
    return identifier


def _is_blank(value):
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False


def _coerce_key(df, key):
    copied = df.copy()
    copied[key] = copied[key].astype(str)
    copied = copied[copied[key].str.strip() != '']
    copied = copied.drop_duplicates(subset=[key], keep='last')
    return copied


def _auto_feishu_cfg():
    from . import load_cfg

    cfg = load_cfg()
    matches = []
    for val in vars(cfg).values():
        if hasattr(val, 'feishu_app_id') and hasattr(val, 'feishu_app_secret'):
            matches.append(val)

    if len(matches) == 1:
        return matches[0]
    if len(matches) == 0:
        raise ValueError('Feishu config is required. Pass it in endpoint tuple or keep one feishu config in cfg.')
    raise ValueError('Multiple feishu configs found. Please pass feishu config in endpoint tuple.')


def _read_endpoint(endpoint):
    if not isinstance(endpoint, tuple) or len(endpoint) < 2:
        raise ValueError('endpoint must be tuple like ("feishu", sheet, cfg?) or ("db", cfg, table)')

    kind = endpoint[0]
    if kind == 'feishu':
        sheet = endpoint[1]
        feishu_cfg = endpoint[2] if len(endpoint) >= 3 else _auto_feishu_cfg()
        return fs_read_df(sheet, feishu_cfg), {'kind': kind, 'sheet': sheet, 'feishu_cfg': feishu_cfg}

    if kind == 'db':
        if len(endpoint) < 3:
            raise ValueError('db endpoint requires ("db", db_cfg, table)')
        db_cfg = endpoint[1]
        table = _safe_identifier(endpoint[2])
        df = query(f'SELECT * FROM {table}', db_cfg)
        return df, {'kind': kind, 'db_cfg': db_cfg, 'table': table}

    raise ValueError(f'Unsupported endpoint kind: {kind}')


def _write_endpoint(endpoint_state, df, key, on_conflict):
    kind = endpoint_state['kind']
    if kind == 'feishu':
        fs_write_df(endpoint_state['sheet'], df, endpoint_state['feishu_cfg'], clear_sheet=True)
        return

    if kind == 'db':
        tb = ManageTable(endpoint_state['table'], endpoint_state['db_cfg'])
        tb.upsert_data(df, ignore=False, key=key)
        return

    raise ValueError(f'Unsupported endpoint kind: {kind}')


def _merge_bidirectional(source_df, target_df, key, on_conflict):
    if on_conflict != 'upsert':
        raise ValueError('on_conflict currently only supports "upsert"')
    if key not in source_df.columns:
        raise ValueError(f'key "{key}" not found in source columns')
    if key not in target_df.columns:
        raise ValueError(f'key "{key}" not found in target columns')

    left = _coerce_key(source_df, key)
    right = _coerce_key(target_df, key)

    ordered_cols = OrderedDict()
    for col in left.columns:
        ordered_cols[col] = True
    for col in right.columns:
        ordered_cols[col] = True

    left_map = left.set_index(key).to_dict(orient='index')
    right_map = right.set_index(key).to_dict(orient='index')
    all_keys = list(OrderedDict.fromkeys(list(left_map.keys()) + list(right_map.keys())))

    merged_rows = []
    for k in all_keys:
        src_row = left_map.get(k, {})
        tgt_row = right_map.get(k, {})
        row = {}
        for col in ordered_cols.keys():
            if col == key:
                row[col] = k
                continue
            src_val = src_row.get(col)
            tgt_val = tgt_row.get(col)
            if not _is_blank(src_val):
                row[col] = src_val
            elif not _is_blank(tgt_val):
                row[col] = tgt_val
            else:
                row[col] = None
        merged_rows.append(row)

    merged_df = pd.DataFrame(merged_rows)
    if key in merged_df.columns:
        merged_df = merged_df[[key] + [c for c in merged_df.columns if c != key]]
    return merged_df


def sync(source, target, on_conflict='upsert', key='id'):
    source_df, source_state = _read_endpoint(source)
    target_df, target_state = _read_endpoint(target)
    merged_df = _merge_bidirectional(source_df, target_df, key=key, on_conflict=on_conflict)
    _write_endpoint(source_state, merged_df, key=key, on_conflict=on_conflict)
    _write_endpoint(target_state, merged_df, key=key, on_conflict=on_conflict)
    logger.info('sync completed, merged rows: %s', len(merged_df))
    return merged_df
