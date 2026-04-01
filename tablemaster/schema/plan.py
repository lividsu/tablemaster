from __future__ import annotations

from pathlib import Path
from typing import Optional

from .models import Plan


def render_plan(plan: Plan, connection_info: Optional[str] = None) -> str:
    title = f'Plan for [{plan.connection}]'
    if connection_info:
        title = f'{title} ({connection_info})'
    lines = [title, '-' * len(title), '']
    if not plan.actions and not plan.warnings:
        lines.append('No changes.')
        return '\n'.join(lines)

    for action in plan.actions:
        target = f'{action.table}.{action.column}' if action.column else action.table
        lines.append(f'+ {action.action} {target}')
        if action.ddl:
            lines.append(f'    {action.ddl}')

    for warning in plan.warnings:
        target = f'{warning.table}.{warning.column}' if warning.column else warning.table
        message = warning.detail.get('message', '')
        lines.append(f'⚠ {warning.action} {target} {message}'.rstrip())

    lines.extend(['', f'Summary: {plan.summary()}'])
    return '\n'.join(lines)


def save_plan(plan: Plan, output: str | Path) -> Path:
    path = Path(output).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(plan.to_json(), encoding='utf-8')
    return path


def load_plan(path: str | Path) -> Plan:
    raw = Path(path).resolve().read_text(encoding='utf-8')
    return Plan.from_json(raw)
