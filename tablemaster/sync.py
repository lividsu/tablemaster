from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import pandas as pd

logger = logging.getLogger(__name__)


@runtime_checkable
class DataFrameEndpoint(Protocol):
    """An endpoint that can participate in a tablemaster sync."""

    @property
    def label(self) -> str: ...

    def read(self) -> pd.DataFrame: ...

    def write(self, df: pd.DataFrame, *, key: str, on_conflict: str) -> None: ...


@dataclass(frozen=True)
class FeishuEndpoint:
    sheet: tuple[str, str] | list[str]
    cfg: Any

    @property
    def label(self) -> str:
        return f'feishu:{self.sheet[0]}/{self.sheet[1]}'

    def read(self) -> pd.DataFrame:
        from .feishu import fs_read_df

        return fs_read_df(self.sheet, self.cfg)

    def write(self, df: pd.DataFrame, *, key: str, on_conflict: str) -> None:
        from .feishu import fs_write_df

        fs_write_df(self.sheet, df, self.cfg, clear_sheet=True)


@dataclass(frozen=True)
class DatabaseEndpoint:
    cfg: Any
    table: str

    def __post_init__(self) -> None:
        # ManageTable performs the same validation before writes. Validate early
        # so sync does not read either side before discovering a bad target.
        from .database import _safe_qualified_identifier

        _safe_qualified_identifier(self.table)

    @property
    def label(self) -> str:
        return f'db:{getattr(self.cfg, "name", "database")}/{self.table}'

    def read(self) -> pd.DataFrame:
        from .database import _quote_table, query

        db_type = getattr(self.cfg, 'db_type', 'mysql').lower()
        return query(f'SELECT * FROM {_quote_table(self.table, db_type)}', self.cfg)

    def write(self, df: pd.DataFrame, *, key: str, on_conflict: str) -> None:
        from .database import ManageTable

        table = ManageTable(self.table, self.cfg)
        table.upsert_data(df, ignore=on_conflict == 'ignore', key=key)


class SyncError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        completed_endpoints: list[str],
        failed_endpoint: str,
        merged: pd.DataFrame,
    ) -> None:
        super().__init__(message)
        self.completed_endpoints = completed_endpoints
        self.failed_endpoint = failed_endpoint
        self.merged = merged


def feishu_endpoint(sheet, cfg) -> FeishuEndpoint:
    return FeishuEndpoint(sheet=sheet, cfg=cfg)


def database_endpoint(cfg, table: str) -> DatabaseEndpoint:
    return DatabaseEndpoint(cfg=cfg, table=table)


def _auto_feishu_cfg():
    from . import load_cfg

    cfg = load_cfg()
    matches = [
        value
        for value in vars(cfg).values()
        if hasattr(value, 'feishu_app_id') and hasattr(value, 'feishu_app_secret')
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError('Feishu config is required; pass it explicitly to FeishuEndpoint.')
    raise ValueError('Multiple Feishu configs found; pass one explicitly to FeishuEndpoint.')


def _coerce_endpoint(endpoint) -> DataFrameEndpoint:
    if isinstance(endpoint, DataFrameEndpoint):
        return endpoint
    if not isinstance(endpoint, tuple) or len(endpoint) < 2:
        raise TypeError('endpoint must implement DataFrameEndpoint or use a legacy endpoint tuple')

    warnings.warn(
        'Tuple sync endpoints are deprecated; use FeishuEndpoint or DatabaseEndpoint.',
        FutureWarning,
        stacklevel=3,
    )
    kind = endpoint[0]
    if kind == 'feishu':
        cfg = endpoint[2] if len(endpoint) >= 3 else _auto_feishu_cfg()
        return FeishuEndpoint(endpoint[1], cfg)
    if kind == 'db' and len(endpoint) >= 3:
        return DatabaseEndpoint(endpoint[1], endpoint[2])
    raise ValueError(f'Unsupported endpoint tuple: {endpoint!r}')


def _is_blank(value) -> bool:
    if value is None:
        return True
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if result is pd.NA:
        return True
    try:
        return bool(result)
    except (TypeError, ValueError):
        return False


def _prepare(df: pd.DataFrame, key: str, endpoint_label: str) -> pd.DataFrame:
    if key not in df.columns:
        raise ValueError(f'key {key!r} not found in {endpoint_label}')
    copied = df.copy()
    non_blank = ~copied[key].map(_is_blank)
    copied = copied.loc[non_blank]
    copied = copied[copied[key].map(lambda value: str(value).strip() != '')]
    canonical = copied[key].map(str)
    return copied.loc[~canonical.duplicated(keep='last')]


def _rows_by_key(df: pd.DataFrame, key: str) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for row in df.to_dict(orient='records'):
        rows[str(row[key])] = row
    return rows


def _choose_row(
    source_row: dict,
    target_row: dict,
    *,
    conflict_policy: str,
    updated_at: str | None,
) -> tuple[dict, dict]:
    if not source_row:
        return target_row, {}
    if not target_row:
        return source_row, {}
    if conflict_policy == 'source_wins':
        return source_row, target_row
    if conflict_policy == 'target_wins':
        return target_row, source_row
    if conflict_policy == 'newest':
        if not updated_at:
            raise ValueError('updated_at is required when conflict_policy="newest"')
        source_time = pd.to_datetime(source_row.get(updated_at), errors='coerce', utc=True)
        target_time = pd.to_datetime(target_row.get(updated_at), errors='coerce', utc=True)
        if pd.isna(source_time) and pd.isna(target_time):
            raise ValueError(f'Neither side has a valid {updated_at!r} value for a conflicting row')
        if pd.isna(target_time) or (not pd.isna(source_time) and source_time >= target_time):
            return source_row, target_row
        return target_row, source_row
    raise ValueError('conflict_policy must be "source_wins", "target_wins", or "newest"')


def _merge_bidirectional(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    key: str,
    on_conflict: str = 'upsert',
    *,
    conflict_policy: str = 'source_wins',
    updated_at: str | None = None,
    source_label: str = 'source',
    target_label: str = 'target',
) -> pd.DataFrame:
    if on_conflict != 'upsert':
        raise ValueError('sync currently requires on_conflict="upsert" so both endpoints converge')

    source = _prepare(source_df, key, source_label)
    target = _prepare(target_df, key, target_label)
    columns = list(dict.fromkeys([*source.columns, *target.columns]))
    source_rows = _rows_by_key(source, key)
    target_rows = _rows_by_key(target, key)
    keys = list(dict.fromkeys([*source_rows, *target_rows]))

    merged_rows = []
    for value in keys:
        preferred, fallback = _choose_row(
            source_rows.get(value, {}),
            target_rows.get(value, {}),
            conflict_policy=conflict_policy,
            updated_at=updated_at,
        )
        preferred_key = preferred.get(key)
        fallback_key = fallback.get(key)
        row = {key: preferred_key if preferred_key is not None else fallback_key}
        for column in columns:
            if column == key:
                continue
            preferred_value = preferred.get(column)
            row[column] = (
                fallback.get(column)
                if _is_blank(preferred_value) and column not in preferred
                else preferred_value
            )
        merged_rows.append(row)

    return pd.DataFrame(merged_rows, columns=[key, *[c for c in columns if c != key]])


def sync(
    source,
    target,
    on_conflict: str = 'upsert',
    key: str = 'id',
    *,
    conflict_policy: str = 'source_wins',
    updated_at: str | None = None,
    delete_policy: str = 'keep',
) -> pd.DataFrame:
    """Merge two endpoints and write the same result to both.

    Writes are not atomic across remote systems. If the second write fails,
    SyncError reports which endpoint was already updated and carries the merged
    DataFrame for recovery.
    """
    if delete_policy != 'keep':
        raise ValueError('delete_policy currently only supports "keep"; deletion requires tombstones')

    source_endpoint = _coerce_endpoint(source)
    target_endpoint = _coerce_endpoint(target)
    source_df = source_endpoint.read()
    target_df = target_endpoint.read()
    merged = _merge_bidirectional(
        source_df,
        target_df,
        key,
        on_conflict,
        conflict_policy=conflict_policy,
        updated_at=updated_at,
        source_label=source_endpoint.label,
        target_label=target_endpoint.label,
    )

    completed: list[str] = []
    for endpoint in (target_endpoint, source_endpoint):
        try:
            endpoint.write(merged, key=key, on_conflict=on_conflict)
            completed.append(endpoint.label)
        except Exception as exc:
            raise SyncError(
                f'sync write failed at {endpoint.label}; completed={completed}',
                completed_endpoints=completed,
                failed_endpoint=endpoint.label,
                merged=merged,
            ) from exc

    logger.info('sync completed, merged rows: %s', len(merged))
    return merged
