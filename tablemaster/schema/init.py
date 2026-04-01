from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml


_CFG_TEMPLATE = """mydb:
  host: 127.0.0.1
  user: root
  password: change_me
  database: demo
  port: 3306
  db_type: mysql
"""


def _is_db_entry(value) -> bool:
    return isinstance(value, dict) and 'host' in value and 'database' in value


def _load_cfg_raw(cfg_path: Path) -> dict:
    if not cfg_path.exists():
        return {}
    with cfg_path.open('r', encoding='utf-8') as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f'Config root must be dict: {cfg_path}')
    return data


def _discover_connections(cfg_data: dict) -> list[str]:
    return sorted([key for key, val in cfg_data.items() if _is_db_entry(val)])


def init_scaffold(
    base_dir: str | Path = '.',
    cfg_path: Optional[str | Path] = None,
    connections: Optional[list[str]] = None,
) -> dict:
    base = Path(base_dir).resolve()
    base.mkdir(parents=True, exist_ok=True)
    cfg_file = Path(cfg_path).resolve() if cfg_path else base / 'cfg.yaml'
    created_cfg = False
    if not cfg_file.exists():
        cfg_file.parent.mkdir(parents=True, exist_ok=True)
        cfg_file.write_text(_CFG_TEMPLATE, encoding='utf-8')
        created_cfg = True
    cfg_data = _load_cfg_raw(cfg_file)
    discovered = _discover_connections(cfg_data)
    target_connections = list(connections) if connections else discovered
    if not target_connections:
        target_connections = ['mydb']

    invalid = [c for c in target_connections if discovered and c not in discovered]
    if invalid:
        raise ValueError(f'Connections not found in cfg.yaml: {", ".join(invalid)}')

    schema_root = base / 'schema'
    schema_root.mkdir(parents=True, exist_ok=True)
    created_paths: list[str] = []
    for conn in target_connections:
        conn_dir = schema_root / conn
        conn_dir.mkdir(parents=True, exist_ok=True)
        keep_file = conn_dir / '.gitkeep'
        if not keep_file.exists():
            keep_file.write_text('', encoding='utf-8')
            created_paths.append(str(keep_file))

    return {
        'cfg_path': str(cfg_file),
        'created_cfg': created_cfg,
        'connections': target_connections,
        'created_paths': created_paths,
    }
