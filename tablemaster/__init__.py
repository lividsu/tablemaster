import warnings
import importlib

from .config import load_cfg


def __getattr__(name: str):
    if name == 'cfg':
        warnings.warn(
            'Implicit loading via tm.cfg is deprecated and will be removed in a future release; use tm.load_cfg() and pass config objects explicitly.',
            FutureWarning,
            stacklevel=2,
        )
        return load_cfg()
    symbol_module_map = {
        ('query', 'opt', 'ManageTable', 'Manage_table'): 'database',
        ('fs_read_df', 'fs_read_base', 'fs_write_df', 'fs_write_base'): 'feishu',
        ('gs_read_df', 'gs_write_df'): 'gspread',
        ('read', 'batch_read', 'read_dfs'): 'local',
        ('sync',): 'sync',
        ('utils',): 'utils',
        ('DBConfig', 'FeishuConfig', 'GoogleConfig', 'ConfigNamespace', 'read_cfg'): 'config',
    }
    for names, module in symbol_module_map.items():
        if name in names:
            mod = importlib.import_module(f'.{module}', __name__)
            return getattr(mod, name)
    raise AttributeError(f"module 'tablemaster' has no attribute {name!r}")
