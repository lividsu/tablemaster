from __future__ import annotations

from typing import Optional

from .dialects import get_dialect
from .models import ActualTable
from ..database import _resolve_engine


def introspect_tables(
    cfg,
    table_names: Optional[list[str]] = None,
    schema_name: Optional[str] = None,
) -> list[ActualTable]:
    dialect = get_dialect(getattr(cfg, 'db_type', 'mysql'))
    engine = _resolve_engine(cfg)
    database = getattr(cfg, 'database', '')
    return dialect.introspect(engine, database, table_names=table_names, schema_name=schema_name)
