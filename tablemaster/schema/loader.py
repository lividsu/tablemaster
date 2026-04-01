from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml

from .models import ColumnDef, IndexDef, TableDef


def _coerce_default(value) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _parse_column(raw: dict) -> ColumnDef:
    if 'name' not in raw or 'type' not in raw:
        raise ValueError('Each column must include name and type')
    return ColumnDef(
        name=str(raw['name']),
        type=str(raw['type']),
        primary_key=bool(raw.get('primary_key', False)),
        nullable=bool(raw.get('nullable', True)),
        default=_coerce_default(raw.get('default')),
        comment=raw.get('comment'),
    )


def _parse_index(raw: dict) -> IndexDef:
    if 'name' not in raw or 'columns' not in raw:
        raise ValueError('Each index must include name and columns')
    columns = raw['columns']
    if not isinstance(columns, list) or not columns:
        raise ValueError('Index columns must be a non-empty list')
    return IndexDef(
        name=str(raw['name']),
        columns=[str(c) for c in columns],
        unique=bool(raw.get('unique', False)),
    )


def parse_table_file(path: Path) -> TableDef:
    with path.open('r', encoding='utf-8') as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError(f'Schema file root must be dict: {path}')
    table_name = raw.get('table') or path.stem
    columns_raw = raw.get('columns') or []
    if not columns_raw:
        raise ValueError(f'Schema file requires non-empty columns: {path}')
    columns = [_parse_column(c) for c in columns_raw]
    indexes = [_parse_index(i) for i in (raw.get('indexes') or [])]
    return TableDef(
        table=str(table_name),
        columns=columns,
        indexes=indexes,
        comment=raw.get('comment'),
        database=raw.get('database'),
        schema_name=raw.get('schema') or raw.get('schema_name'),
    )


def load_schema_definitions(
    connection: str,
    root_dir: str | Path = 'schema',
    table: Optional[str] = None,
) -> list[TableDef]:
    root = Path(root_dir).resolve()
    conn_dir = root / connection
    if not conn_dir.exists() or not conn_dir.is_dir():
        raise FileNotFoundError(f'Schema directory not found: {conn_dir}')
    files = sorted(conn_dir.rglob('*.yaml')) + sorted(conn_dir.rglob('*.yml'))
    defs: list[TableDef] = []
    for file in files:
        parsed = parse_table_file(file)
        if table and parsed.table != table:
            continue
        defs.append(parsed)
    if table and not defs:
        raise FileNotFoundError(f'Table schema not found under {conn_dir}: {table}')
    return defs
