from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml

from .models import ActualTable


def _table_to_payload(table: ActualTable) -> dict:
    payload: dict = {
        'table': table.table,
        'columns': [],
    }
    if table.comment:
        payload['comment'] = table.comment
    for col in table.columns:
        item = {
            'name': col.name,
            'type': col.type,
            'nullable': bool(col.nullable),
        }
        if col.primary_key:
            item['primary_key'] = True
        if col.default is not None:
            item['default'] = str(col.default)
        if col.comment:
            item['comment'] = col.comment
        payload['columns'].append(item)
    if table.indexes:
        payload['indexes'] = [
            {
                'name': idx.name,
                'columns': list(idx.columns),
                'unique': bool(idx.unique),
            }
            for idx in table.indexes
        ]
    return payload


def write_pulled_schema(
    tables: list[ActualTable],
    output_dir: str | Path,
) -> list[Path]:
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table in tables:
        target = out / f'{table.table}.yaml'
        payload = _table_to_payload(table)
        with target.open('w', encoding='utf-8') as f:
            yaml.safe_dump(payload, f, sort_keys=False, allow_unicode=True)
        written.append(target)
    return written


def pull_schema(
    cfg,
    introspect_func,
    connection: str,
    output_dir: str | Path = 'schema',
    table: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> list[Path]:
    tables = introspect_func(
        cfg,
        table_names=[table] if table else None,
        schema_name=schema_name,
    )
    target_dir = Path(output_dir).resolve() / connection
    return write_pulled_schema(tables, target_dir)
