from __future__ import annotations

import re

from sqlalchemy import inspect

from ..models import ActualColumn, ActualTable, ColumnDef, IndexDef, TableDef
from .base import BaseDialect


def _quote(value: str) -> str:
    return f'"{value}"'


def _quote_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class PostgreSQLDialect(BaseDialect):
    def map_type(self, generic_type: str) -> str:
        normalized = generic_type.strip().upper()
        if normalized.startswith('DECIMAL'):
            return normalized.replace('DECIMAL', 'NUMERIC', 1)
        if normalized.startswith('BOOLEAN'):
            return 'BOOLEAN'
        if normalized == 'JSON':
            return 'JSONB'
        return normalized

    def normalize_type(self, db_type: str) -> str:
        v = db_type.strip().lower()
        if v.startswith('character varying'):
            m = re.match(r'character varying\((\d+)\)', v)
            if m:
                return f'VARCHAR({m.group(1)})'
            return 'VARCHAR'
        if v == 'integer':
            return 'INT'
        if v.startswith('numeric'):
            m = re.match(r'numeric\((\d+),\s*(\d+)\)', v)
            if m:
                return f'DECIMAL({m.group(1)},{m.group(2)})'
            return 'DECIMAL'
        if v == 'timestamp without time zone':
            return 'TIMESTAMP'
        if v == 'jsonb':
            return 'JSON'
        return v.upper()

    def introspect(
        self,
        engine,
        database: str,
        table_names: list[str] | None = None,
        schema_name: str | None = None,
    ) -> list[ActualTable]:
        inspector = inspect(engine)
        schema = schema_name or 'public'
        names = table_names or inspector.get_table_names(schema=schema)
        results: list[ActualTable] = []
        for name in names:
            columns = []
            pk_cols = set(inspector.get_pk_constraint(name, schema=schema).get('constrained_columns') or [])
            for col in inspector.get_columns(name, schema=schema):
                columns.append(
                    ActualColumn(
                        name=col['name'],
                        type=str(col['type']),
                        nullable=bool(col.get('nullable', True)),
                        default=None if col.get('default') is None else str(col.get('default')),
                        comment=col.get('comment'),
                        primary_key=col['name'] in pk_cols,
                    )
                )
            indexes: list[IndexDef] = []
            for idx in inspector.get_indexes(name, schema=schema):
                indexes.append(
                    IndexDef(
                        name=idx['name'],
                        columns=list(idx.get('column_names') or []),
                        unique=bool(idx.get('unique', False)),
                    )
                )
            table_comment = None
            try:
                table_comment = (inspector.get_table_comment(name, schema=schema) or {}).get('text')
            except Exception:
                table_comment = None
            results.append(
                ActualTable(table=name, columns=columns, indexes=indexes, comment=table_comment, schema_name=schema)
            )
        return results

    def _qualified_table(self, table: str, schema_name: str | None = None) -> str:
        schema = schema_name or 'public'
        return f'{_quote(schema)}.{_quote(table)}'

    def _column_sql(self, col: ColumnDef) -> str:
        sql = f'{_quote(col.name)} {self.map_type(col.type)}'
        if not col.nullable or col.primary_key:
            sql += ' NOT NULL'
        if col.default is not None:
            sql += f' DEFAULT {col.default}'
        return sql

    def gen_create_table(self, table: TableDef) -> str:
        schema = table.schema_name or 'public'
        pieces = [self._column_sql(col) for col in table.columns]
        pk_cols = [col.name for col in table.columns if col.primary_key]
        if pk_cols:
            cols_sql = ', '.join(_quote(c) for c in pk_cols)
            pieces.append(f'PRIMARY KEY ({cols_sql})')
        body = ', '.join(pieces)
        return f'CREATE TABLE {self._qualified_table(table.table, schema)} ({body})'

    def gen_add_column(self, table: str, col: ColumnDef, schema_name: str | None = None) -> str:
        return f'ALTER TABLE {self._qualified_table(table, schema_name)} ADD COLUMN {self._column_sql(col)}'

    def gen_alter_column_type(
        self,
        table: str,
        col_name: str,
        new_type: str,
        schema_name: str | None = None,
    ) -> str:
        return (
            f'ALTER TABLE {self._qualified_table(table, schema_name)} '
            f'ALTER COLUMN {_quote(col_name)} TYPE {new_type}'
        )

    def gen_alter_column_nullable(
        self,
        table: str,
        col_name: str,
        nullable: bool,
        col_type: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        action = 'DROP NOT NULL' if nullable else 'SET NOT NULL'
        return (
            f'ALTER TABLE {self._qualified_table(table, schema_name)} '
            f'ALTER COLUMN {_quote(col_name)} {action}'
        )

    def gen_alter_column_default(
        self,
        table: str,
        col_name: str,
        default: str | None,
        schema_name: str | None = None,
    ) -> str:
        if default is None:
            action = 'DROP DEFAULT'
        else:
            action = f'SET DEFAULT {default}'
        return (
            f'ALTER TABLE {self._qualified_table(table, schema_name)} '
            f'ALTER COLUMN {_quote(col_name)} {action}'
        )

    def gen_alter_column_comment(
        self,
        table: str,
        col_name: str,
        comment: str | None,
        col_type: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        rendered = "''" if comment is None else _quote_str(comment)
        return f'COMMENT ON COLUMN {self._qualified_table(table, schema_name)}.{_quote(col_name)} IS {rendered}'

    def gen_alter_table_comment(
        self,
        table: str,
        comment: str | None,
        schema_name: str | None = None,
    ) -> str:
        rendered = "''" if comment is None else _quote_str(comment)
        return f'COMMENT ON TABLE {self._qualified_table(table, schema_name)} IS {rendered}'

    def gen_create_index(self, table: str, index: IndexDef, schema_name: str | None = None) -> str:
        unique = 'UNIQUE ' if index.unique else ''
        cols_sql = ', '.join(_quote(c) for c in index.columns)
        return (
            f'CREATE {unique}INDEX {_quote(index.name)} '
            f'ON {self._qualified_table(table, schema_name)} ({cols_sql})'
        )

    def gen_drop_index(self, table: str, index_name: str, schema_name: str | None = None) -> str:
        schema = schema_name or 'public'
        return f'DROP INDEX {_quote(schema)}.{_quote(index_name)}'
