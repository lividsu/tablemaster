from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from ..database import opt
from .models import Plan


@dataclass
class ApplyItemResult:
    action: str
    table: str
    column: str | None
    ddl: str
    success: bool
    error: str = ''


@dataclass
class ApplyResult:
    connection: str
    executed: list[ApplyItemResult] = field(default_factory=list)
    skipped_warnings: int = 0

    def ok(self) -> bool:
        return all(item.success for item in self.executed)

    def summary(self) -> str:
        success = sum(1 for x in self.executed if x.success)
        failed = sum(1 for x in self.executed if not x.success)
        return f'success:{success}, failed:{failed}, warnings:{self.skipped_warnings}'


def apply_plan(
    plan: Plan,
    cfg,
    auto_approve: bool = False,
    continue_on_error: bool = False,
    confirm: Callable[[str], bool] | None = None,
    report: Callable[[str], None] | None = None,
) -> ApplyResult:
    result = ApplyResult(connection=plan.connection, skipped_warnings=len(plan.warnings))
    executable = [a for a in plan.actions if not a.is_warning and a.ddl]
    if not executable:
        return result

    if not auto_approve:
        confirmed = bool(confirm and confirm(
            f'Execute {len(executable)} DDL statements for {plan.connection}?'
        ))
        if not confirmed:
            return result

    for action in executable:
        try:
            opt(action.ddl, cfg)
            result.executed.append(
                ApplyItemResult(
                    action=action.action,
                    table=action.table,
                    column=action.column,
                    ddl=action.ddl,
                    success=True,
                )
            )
            if report:
                report(f'✓ {action.action} {action.table}')
        except Exception as exc:
            result.executed.append(
                ApplyItemResult(
                    action=action.action,
                    table=action.table,
                    column=action.column,
                    ddl=action.ddl,
                    success=False,
                    error=str(exc),
                )
            )
            if report:
                report(f'✗ {action.action} {action.table}: {exc}')
            if auto_approve and not continue_on_error:
                break
            if not auto_approve:
                should_continue = bool(confirm and confirm('DDL failed. Continue?'))
                if not should_continue:
                    break

    return result
