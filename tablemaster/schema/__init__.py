from __future__ import annotations

import importlib


_SYMBOL_MODULE_MAP = {
    'ApplyResult': 'apply',
    'apply_plan': 'apply',
    'generate_plan': 'diff',
    'init_scaffold': 'init',
    'introspect_tables': 'introspect',
    'load_ignored_tables': 'loader',
    'load_schema_definitions': 'loader',
    'load_plan': 'plan',
    'render_plan': 'plan',
    'save_plan': 'plan',
    'pull_schema': 'pull',
    'write_pulled_schema': 'pull',
    'ActualColumn': 'models',
    'ActualTable': 'models',
    'ColumnDef': 'models',
    'IndexDef': 'models',
    'Plan': 'models',
    'PlanAction': 'models',
    'TableDef': 'models',
}

__all__ = list(_SYMBOL_MODULE_MAP)


def __getattr__(name: str):
    module_name = _SYMBOL_MODULE_MAP.get(name)
    if not module_name:
        raise AttributeError(f"module 'tablemaster.schema' has no attribute {name!r}")
    module = importlib.import_module(f'.{module_name}', __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
