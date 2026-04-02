from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from tablemaster.database import ManageTable
from tablemaster.feishu import fs_write_base


class _DummyResponse:
    def __init__(self, body, status_code=200):
        self._body = body
        self.status_code = status_code

    def json(self):
        return self._body


class ErrorVisibilityTests(TestCase):
    def setUp(self):
        self.db_cfg = SimpleNamespace(
            name='test_db',
            user='u',
            password='p',
            host='127.0.0.1',
            database='d',
            db_type='mysql',
        )
        self.feishu_cfg = SimpleNamespace(feishu_app_id='id', feishu_app_secret='secret')

    def test_manage_table_exists_propagates_errors(self):
        table = ManageTable('orders', self.db_cfg)
        with patch('tablemaster.database._resolve_engine', side_effect=RuntimeError('db unavailable')):
            with self.assertRaises(RuntimeError):
                table.exists()

    def test_delete_table_propagates_errors(self):
        table = ManageTable('orders', self.db_cfg)
        with patch('tablemaster.database.opt', side_effect=RuntimeError('drop failed')):
            with self.assertRaises(RuntimeError):
                table.delete_table()

    def test_fs_write_base_raises_when_batch_write_failed(self):
        df = pd.DataFrame({'a': [1]})

        with patch('tablemaster.feishu._get_tenant_access_token', return_value='token'):
            with patch('tablemaster.feishu._get_bitable_fields', return_value={'a'}):
                with patch(
                    'tablemaster.feishu._request_with_retry',
                    return_value=_DummyResponse({'code': 1001, 'msg': 'bad request'}),
                ):
                    with self.assertRaises(RuntimeError):
                        fs_write_base(['app_token', 'table_id'], df, self.feishu_cfg)
