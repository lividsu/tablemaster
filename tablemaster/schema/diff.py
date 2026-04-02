from __future__ import annotations

from .dialects.base import BaseDialect
from .models import ActualTable, ColumnDef, Plan, PlanAction, TableDef


def _norm_default(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().strip("'").upper()


def _action(
    action: str,
    table: str,
    ddl: str = '',
    column: str | None = None,
    detail: dict | None = None,
    is_warning: bool = False,
) -> PlanAction:
    return PlanAction(
        action=action,
        table=table,
        column=column,
        detail=detail or {},
        ddl=ddl,
        is_warning=is_warning,
    )


def _mapped_desired_type(dialect: BaseDialect, col: ColumnDef) -> str:
    return dialect.normalize_type(dialect.map_type(col.type))


def generate_plan(
    connection_name: str,
    desired: list[TableDef],
    actual: list[ActualTable],
    dialect: BaseDialect,
) -> Plan:
    plan = Plan(connection=connection_name)
    desired_map = {t.table: t for t in desired}
    actual_map = {t.table: t for t in actual}

    for table_name, table_def in desired_map.items():
        current = actual_map.get(table_name)
        if current is None:
            plan.actions.append(
                _action(
                    'CREATE_TABLE',
                    table_name,
                    ddl=dialect.gen_create_table(table_def),
                    detail={'table': table_name},
                )
            )
            for idx in table_def.indexes:
                plan.actions.append(
                    _action(
                        'CREATE_INDEX',
                        table_name,
                        ddl=dialect.gen_create_index(table_name, idx, schema_name=table_def.schema_name),
                        detail={'index': idx.name},
                    )
                )
            if table_def.comment:
                plan.actions.append(
                    _action(
                        'ALTER_TABLE_COMMENT',
                        table_name,
                        ddl=dialect.gen_alter_table_comment(
                            table_name,
                            table_def.comment,
                            schema_name=table_def.schema_name,
                        ),
                    )
                )
            for col in table_def.columns:
                if col.comment:
                    plan.actions.append(
                        _action(
                            'ALTER_COLUMN_COMMENT',
                            table_name,
                            column=col.name,
                            ddl=dialect.gen_alter_column_comment(
                                table_name,
                                col.name,
                                col.comment,
                                col_type=dialect.map_type(col.type),
                                schema_name=table_def.schema_name,
                            ),
                        )
                    )
            continue

        desired_cols = {c.name: c for c in table_def.columns}
        actual_cols = {c.name: c for c in current.columns}
        desired_pk = [c.name for c in table_def.columns if c.primary_key]
        actual_pk = list(current.primary_key_columns or [c.name for c in current.columns if c.primary_key])
        primary_key_changed = desired_pk != actual_pk

        if primary_key_changed and actual_pk:
            plan.actions.append(
                _action(
                    'DROP_PRIMARY_KEY',
                    table_name,
                    ddl=dialect.gen_drop_primary_key(
                        table_name,
                        primary_key_name=current.primary_key_name,
                        schema_name=table_def.schema_name,
                    ),
                    detail={'old': actual_pk, 'new': desired_pk},
                )
            )

        for col_name, desired_col in desired_cols.items():
            actual_col = actual_cols.get(col_name)
            if actual_col is None:
                plan.actions.append(
                    _action(
                        'ADD_COLUMN',
                        table_name,
                        column=col_name,
                        ddl=dialect.gen_add_column(table_name, desired_col, schema_name=table_def.schema_name),
                    )
                )
                continue

            desired_type = _mapped_desired_type(dialect, desired_col)
            actual_type = dialect.normalize_type(actual_col.type)
            if desired_type != actual_type:
                plan.actions.append(
                    _action(
                        'ALTER_COLUMN_TYPE',
                        table_name,
                        column=col_name,
                        ddl=dialect.gen_alter_column_type(
                            table_name,
                            col_name,
                            dialect.map_type(desired_col.type),
                            schema_name=table_def.schema_name,
                        ),
                        detail={'old': actual_col.type, 'new': desired_col.type},
                    )
                )

            desired_nullable = bool(desired_col.nullable) and not bool(desired_col.primary_key)
            if desired_nullable != bool(actual_col.nullable):
                plan.actions.append(
                    _action(
                        'ALTER_COLUMN_NULLABLE',
                        table_name,
                        column=col_name,
                        ddl=dialect.gen_alter_column_nullable(
                            table_name,
                            col_name,
                            desired_nullable,
                            col_type=dialect.map_type(desired_col.type),
                            schema_name=table_def.schema_name,
                        ),
                        detail={'old': actual_col.nullable, 'new': desired_nullable},
                    )
                )

            if _norm_default(desired_col.default) != _norm_default(actual_col.default):
                plan.actions.append(
                    _action(
                        'ALTER_COLUMN_DEFAULT',
                        table_name,
                        column=col_name,
                        ddl=dialect.gen_alter_column_default(
                            table_name,
                            col_name,
                            desired_col.default,
                            schema_name=table_def.schema_name,
                        ),
                        detail={'old': actual_col.default, 'new': desired_col.default},
                    )
                )

            desired_comment = (desired_col.comment or '').strip()
            actual_comment = (actual_col.comment or '').strip()
            if desired_comment != actual_comment:
                plan.actions.append(
                    _action(
                        'ALTER_COLUMN_COMMENT',
                        table_name,
                        column=col_name,
                        ddl=dialect.gen_alter_column_comment(
                            table_name,
                            col_name,
                            desired_col.comment,
                            col_type=dialect.map_type(desired_col.type),
                            schema_name=table_def.schema_name,
                        ),
                        detail={'old': actual_col.comment, 'new': desired_col.comment},
                    )
                )

        for col_name in actual_cols:
            if col_name not in desired_cols:
                plan.warnings.append(
                    _action(
                        'COLUMN_MISSING',
                        table_name,
                        column=col_name,
                        is_warning=True,
                        detail={'message': f'{table_name}.{col_name} exists in DB but not in schema'},
                    )
                )

        if primary_key_changed and desired_pk:
            plan.actions.append(
                _action(
                    'ADD_PRIMARY_KEY',
                    table_name,
                    ddl=dialect.gen_add_primary_key(
                        table_name,
                        desired_pk,
                        primary_key_name=current.primary_key_name,
                        schema_name=table_def.schema_name,
                    ),
                    detail={'old': actual_pk, 'new': desired_pk},
                )
            )

        desired_indexes = {idx.name: idx for idx in table_def.indexes}
        actual_indexes = {idx.name: idx for idx in current.indexes}

        for idx_name, idx in desired_indexes.items():
            current_idx = actual_indexes.get(idx_name)
            if current_idx is None:
                plan.actions.append(
                    _action(
                        'CREATE_INDEX',
                        table_name,
                        ddl=dialect.gen_create_index(table_name, idx, schema_name=table_def.schema_name),
                        detail={'index': idx_name},
                    )
                )
            elif (list(idx.columns), bool(idx.unique)) != (list(current_idx.columns), bool(current_idx.unique)):
                plan.actions.append(
                    _action(
                        'DROP_INDEX',
                        table_name,
                        ddl=dialect.gen_drop_index(table_name, idx_name, schema_name=table_def.schema_name),
                        detail={'index': idx_name},
                    )
                )
                plan.actions.append(
                    _action(
                        'CREATE_INDEX',
                        table_name,
                        ddl=dialect.gen_create_index(table_name, idx, schema_name=table_def.schema_name),
                        detail={'index': idx_name},
                    )
                )

        for idx_name in actual_indexes:
            if idx_name not in desired_indexes:
                plan.actions.append(
                    _action(
                        'DROP_INDEX',
                        table_name,
                        ddl=dialect.gen_drop_index(table_name, idx_name, schema_name=table_def.schema_name),
                        detail={'index': idx_name},
                    )
                )

        desired_t_comment = (table_def.comment or '').strip()
        actual_t_comment = (current.comment or '').strip()
        if desired_t_comment != actual_t_comment:
            plan.actions.append(
                _action(
                    'ALTER_TABLE_COMMENT',
                    table_name,
                    ddl=dialect.gen_alter_table_comment(
                        table_name,
                        table_def.comment,
                        schema_name=table_def.schema_name,
                    ),
                    detail={'old': current.comment, 'new': table_def.comment},
                )
            )

    for table_name in actual_map:
        if table_name not in desired_map:
            plan.warnings.append(
                _action(
                    'TABLE_MISSING',
                    table_name,
                    is_warning=True,
                    detail={'message': f'{table_name} exists in DB but not in schema'},
                )
            )

    return plan
