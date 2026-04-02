from __future__ import annotations

import re

from sqlalchemy import inspect

from ..models import ActualColumn, ActualTable, ColumnDef, IndexDef, TableDef
from .base import BaseDialect


def _quote(value: str) -> str:
    return f"`{value}`"


def _quote_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class MySQLDialect(BaseDialect):
    def map_type(self, generic_type: str) -> str:
        normalized = generic_type.strip().upper()
        if normalized.startswith('TIMESTAMP'):
            return 'DATETIME'
        if normalized.startswith('BOOLEAN'):
            return 'TINYINT(1)'
        return normalized

    def normalize_type(self, db_type: str) -> str:
        v = db_type.strip().lower()
        if v.startswith('int('):
            return 'INT'
        if v.startswith('tinyint(1)'):
            return 'BOOLEAN'
        if v.startswith('datetime'):
            return 'DATETIME'
        if v.startswith('decimal'):
            m = re.match(r'decimal\((\d+),(\d+)\)', v)
            if m:
                return f'DECIMAL({m.group(1)},{m.group(2)})'
            return 'DECIMAL'
        if v.startswith('varchar'):
            m = re.match(r'varchar\((\d+)\)', v)
            if m:
                return f'VARCHAR({m.group(1)})'
            return 'VARCHAR'
        return v.upper()

    def introspect(
        self,
        engine,
        database: str,
        table_names: list[str] | None = None,
        schema_name: str | None = None,
    ) -> list[ActualTable]:
        inspector = inspect(engine)
        names = table_names or inspector.get_table_names(schema=database)
        results: list[ActualTable] = []
        for name in names:
            columns = []
            pk_constraint = inspector.get_pk_constraint(name, schema=database) or {}
            pk_column_list = list(pk_constraint.get('constrained_columns') or [])
            pk_cols = set(pk_column_list)
            for col in inspector.get_columns(name, schema=database):
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
            for idx in inspector.get_indexes(name, schema=database):
                indexes.append(
                    IndexDef(
                        name=idx['name'],
                        columns=list(idx.get('column_names') or []),
                        unique=bool(idx.get('unique', False)),
                    )
                )
            table_comment = None
            try:
                table_comment = (inspector.get_table_comment(name, schema=database) or {}).get('text')
            except Exception:
                table_comment = None
            results.append(
                ActualTable(
                    table=name,
                    columns=columns,
                    indexes=indexes,
                    comment=table_comment,
                    primary_key_columns=pk_column_list,
                    primary_key_name=pk_constraint.get('name'),
                )
            )
        return results

    def _qualified_table(self, table: str, schema_name: str | None = None) -> str:
        if schema_name:
            return f'{_quote(schema_name)}.{_quote(table)}'
        return _quote(table)

    def _column_sql(self, col: ColumnDef) -> str:
        sql = f'{_quote(col.name)} {self.map_type(col.type)}'
        if not col.nullable or col.primary_key:
            sql += ' NOT NULL'
        if col.default is not None:
            sql += f' DEFAULT {col.default}'
        if col.comment:
            sql += f' COMMENT {_quote_str(col.comment)}'
        return sql

    def gen_create_table(self, table: TableDef) -> str:
        pieces = [self._column_sql(col) for col in table.columns]
        pk_cols = [col.name for col in table.columns if col.primary_key]
        if pk_cols:
            cols_sql = ', '.join(_quote(c) for c in pk_cols)
            pieces.append(f'PRIMARY KEY ({cols_sql})')
        body = ', '.join(pieces)
        sql = f'CREATE TABLE {self._qualified_table(table.table, table.database)} ({body})'
        if table.comment:
            sql += f' COMMENT={_quote_str(table.comment)}'
        return sql

    def gen_add_column(self, table: str, col: ColumnDef, schema_name: str | None = None) -> str:
        return f'ALTER TABLE {self._qualified_table(table, schema_name)} ADD COLUMN {self._column_sql(col)}'

    def gen_alter_column_type(
        self,
        table: str,
        col_name: str,
        new_type: str,
        schema_name: str | None = None,
    ) -> str:
        return f'ALTER TABLE {self._qualified_table(table, schema_name)} MODIFY COLUMN {_quote(col_name)} {new_type}'

    def gen_alter_column_nullable(
        self,
        table: str,
        col_name: str,
        nullable: bool,
        col_type: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        if not col_type:
            raise ValueError('MySQL ALTER COLUMN NULLABLE requires column type')
        null_sql = 'NULL' if nullable else 'NOT NULL'
        return (
            f'ALTER TABLE {self._qualified_table(table, schema_name)} '
            f'MODIFY COLUMN {_quote(col_name)} {col_type} {null_sql}'
        )

    def gen_alter_column_default(
        self,
        table: str,
        col_name: str,
        default: str | None,
        schema_name: str | None = None,
    ) -> str:
        if default is None:
            return f'ALTER TABLE {self._qualified_table(table, schema_name)} ALTER COLUMN {_quote(col_name)} DROP DEFAULT'
        return (
            f'ALTER TABLE {self._qualified_table(table, schema_name)} '
            f'ALTER COLUMN {_quote(col_name)} SET DEFAULT {default}'
        )

    def gen_alter_column_comment(
        self,
        table: str,
        col_name: str,
        comment: str | None,
        col_type: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        if not col_type:
            raise ValueError('MySQL ALTER COLUMN COMMENT requires column type')
        rendered = 'NULL' if comment is None else _quote_str(comment)
        return (
            f'ALTER TABLE {self._qualified_table(table, schema_name)} '
            f'MODIFY COLUMN {_quote(col_name)} {col_type} COMMENT {rendered}'
        )

    def gen_alter_table_comment(
        self,
        table: str,
        comment: str | None,
        schema_name: str | None = None,
    ) -> str:
        rendered = "''" if comment is None else _quote_str(comment)
        return f'ALTER TABLE {self._qualified_table(table, schema_name)} COMMENT={rendered}'

    def gen_create_index(self, table: str, index: IndexDef, schema_name: str | None = None) -> str:
        unique = 'UNIQUE ' if index.unique else ''
        cols_sql = ', '.join(_quote(c) for c in index.columns)
        return (
            f'CREATE {unique}INDEX {_quote(index.name)} '
            f'ON {self._qualified_table(table, schema_name)} ({cols_sql})'
        )

    def gen_drop_index(self, table: str, index_name: str, schema_name: str | None = None) -> str:
        return f'DROP INDEX {_quote(index_name)} ON {self._qualified_table(table, schema_name)}'

    def gen_drop_primary_key(
        self,
        table: str,
        primary_key_name: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        return f'ALTER TABLE {self._qualified_table(table, schema_name)} DROP PRIMARY KEY'

    def gen_add_primary_key(
        self,
        table: str,
        columns: list[str],
        primary_key_name: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        cols_sql = ', '.join(_quote(c) for c in columns)
        return f'ALTER TABLE {self._qualified_table(table, schema_name)} ADD PRIMARY KEY ({cols_sql})'
