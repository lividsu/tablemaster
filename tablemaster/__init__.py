
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import json
from types import SimpleNamespace
import pandas as pd


with open('cfg.yaml') as cfg:
    cfg = json.loads(json.dumps(load(cfg, Loader=Loader)), object_hook=lambda d: SimpleNamespace(**d))

from . import utils

from .mysql import (
    query,
    opt,
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
)

