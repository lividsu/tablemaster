import os
import warnings
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml


@dataclass
class DBConfig:
    host: str
    user: str
    password: str
    database: str
    name: str = ''
    port: int = 3306
    db_type: str = 'mysql'
    use_ssl: bool = False
    ssl_ca: Optional[str] = None
    ssl_verify_cert: bool = True
    ssl_verify_identity: bool = True
    connect_args: dict[str, Any] = field(default_factory=dict)


@dataclass
class FeishuConfig:
    feishu_app_id: str
    feishu_app_secret: str


@dataclass
class GoogleConfig:
    service_account_path: str


class ConfigNamespace:
    def __init__(self, raw: dict):
        for key, val in raw.items():
            setattr(self, key, _parse_entry(key, val))


def _resolve_cfg_path(path: str = None) -> str:
    explicit_candidates = []
    if path:
        if os.path.isdir(path):
            explicit_candidates.append(os.path.join(path, 'cfg.yaml'))
        explicit_candidates.append(path)
        for candidate in explicit_candidates:
            if candidate and os.path.isfile(candidate):
                return os.path.abspath(candidate)
        raise FileNotFoundError(f'Config file not found: {path}')

    candidates = []
    env_path = os.getenv('TM_CFG_PATH')
    if env_path:
        if os.path.isdir(env_path):
            candidates.append(os.path.join(env_path, 'cfg.yaml'))
        candidates.append(env_path)
    candidates.append(os.path.join(os.getcwd(), 'cfg.yaml'))
    candidates.append(os.path.expanduser('~/.tablemaster/cfg.yaml'))

    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return os.path.abspath(candidate)
    raise FileNotFoundError(
        'Config file not found. Checked: TM_CFG_PATH, ./cfg.yaml, ~/.tablemaster/cfg.yaml'
    )


def _parse_entry(key: str, val):
    if not isinstance(val, dict):
        return val

    if 'host' in val and 'database' in val:
        unknown = set(val) - set(DBConfig.__dataclass_fields__)
        if unknown:
            raise ValueError(f'Unknown database config fields for {key}: {", ".join(sorted(unknown))}')
        db_kwargs = dict(val)
        db_kwargs['name'] = key
        return DBConfig(**db_kwargs)

    if 'feishu_app_id' in val and 'feishu_app_secret' in val:
        unknown = set(val) - set(FeishuConfig.__dataclass_fields__)
        if unknown:
            raise ValueError(f'Unknown Feishu config fields for {key}: {", ".join(sorted(unknown))}')
        fs_kwargs = dict(val)
        return FeishuConfig(**fs_kwargs)

    if 'service_account_path' in val:
        unknown = set(val) - set(GoogleConfig.__dataclass_fields__)
        if unknown:
            raise ValueError(f'Unknown Google config fields for {key}: {", ".join(sorted(unknown))}')
        gs_kwargs = dict(val)
        return GoogleConfig(**gs_kwargs)

    return ConfigNamespace(val)


def load_cfg(path: str = None) -> ConfigNamespace:
    cfg_path = _resolve_cfg_path(path)
    with open(cfg_path, 'r', encoding='utf-8') as f:
        yaml_content = yaml.safe_load(f) or {}
    if not isinstance(yaml_content, dict):
        raise ValueError(f'Config root must be a dict, got: {type(yaml_content).__name__}')
    return ConfigNamespace(yaml_content)


def read_cfg(file_path: str):
    warnings.warn(
        'read_cfg is deprecated and will be removed in a future release; use load_cfg(path) instead.',
        FutureWarning,
        stacklevel=2,
    )
    return load_cfg(file_path)
