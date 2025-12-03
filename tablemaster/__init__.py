from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import json
from types import SimpleNamespace
import pandas as pd

# ============ 配置读取工具 ============

def read_cfg(file_path: str) -> SimpleNamespace:
    """
    从指定路径读取 yaml 配置文件，返回 SimpleNamespace 对象
    支持通过属性访问配置项，如: cfg.database.host
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        yaml_content = load(f, Loader=Loader)
        json_content = json.dumps(yaml_content)
        return json.loads(json_content, object_hook=lambda d: SimpleNamespace(**d))


# ============ 自动加载本地 cfg.yaml ============

cfg = None
try:
    cfg = read_cfg('cfg.yaml')
    print('[✓] cfg.yaml loaded')
except FileNotFoundError:
    pass  # 静默处理，不存在就不加载
except Exception:
    pass  # 其他错误也静默

# ============ 模块导入 ============

from . import utils

from .mysql import (
    query,
    opt,
    ManageTable,
    Manage_table,
)

from .gspread import (
    gs_read_df,
    gs_write_df,
)

from .local import (
    read,
    batch_read,
    read_dfs,
)

from .feishu import (
    fs_read_df,
    fs_read_base,
    fs_write_df,
    fs_write_base,
)