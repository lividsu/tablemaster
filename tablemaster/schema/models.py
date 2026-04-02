from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class ColumnDef:
    name: str
    type: str
    primary_key: bool = False
    nullable: bool = True
    default: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class IndexDef:
    name: str
    columns: list[str]
    unique: bool = False


@dataclass
class TableDef:
    table: str
    columns: list[ColumnDef]
    indexes: list[IndexDef] = field(default_factory=list)
    comment: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None


@dataclass
class ActualColumn:
    name: str
    type: str
    nullable: bool
    default: Optional[str]
    comment: Optional[str]
    primary_key: bool = False


@dataclass
class ActualTable:
    table: str
    columns: list[ActualColumn]
    indexes: list[IndexDef]
    comment: Optional[str] = None
    schema_name: Optional[str] = None
    primary_key_columns: list[str] = field(default_factory=list)
    primary_key_name: Optional[str] = None


@dataclass
class PlanAction:
    action: str
    table: str
    column: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)
    ddl: str = ''
    is_warning: bool = False


@dataclass
class Plan:
    connection: str
    actions: list[PlanAction] = field(default_factory=list)
    warnings: list[PlanAction] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def has_changes(self) -> bool:
        return bool(self.actions)

    def summary(self) -> str:
        counts: dict[str, int] = {}
        for act in self.actions:
            counts[act.action] = counts.get(act.action, 0) + 1
        details = ', '.join(f'{k}:{v}' for k, v in sorted(counts.items()))
        if not details:
            details = 'no executable changes'
        return f'{details}; warnings:{len(self.warnings)}'

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'Plan':
        actions = [PlanAction(**x) for x in data.get('actions', [])]
        warnings = [PlanAction(**x) for x in data.get('warnings', [])]
        return cls(
            connection=data['connection'],
            actions=actions,
            warnings=warnings,
            created_at=data.get('created_at') or datetime.now(timezone.utc).isoformat(),
        )

    @classmethod
    def from_json(cls, raw: str) -> 'Plan':
        return cls.from_dict(json.loads(raw))
