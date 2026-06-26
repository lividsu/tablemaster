from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from tablemaster.config import load_cfg
from tablemaster.database import ManageTable, _build_conn_str, get_connect_args


class _BeginContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class _Engine:
    def begin(self):
        return _BeginContext()


class ConfigAndDatabaseTests(TestCase):
    def test_database_config_keeps_connect_args(self):
        with TemporaryDirectory() as td:
            path = Path(td) / 'cfg.yaml'
            path.write_text(
                '\n'.join(
                    [
                        'db:',
                        '  host: localhost',
                        '  user: user',
                        '  password: pass',
                        '  database: demo',
                        '  connect_args:',
                        '    connect_timeout: 5',
                    ]
                ),
                encoding='utf-8',
            )
            cfg = load_cfg(path)
            self.assertEqual({'connect_timeout': 5}, cfg.db.connect_args)

    def test_database_config_rejects_unknown_fields(self):
        with TemporaryDirectory() as td:
            path = Path(td) / 'cfg.yaml'
            path.write_text(
                'db:\n  host: localhost\n  user: u\n  password: p\n  database: d\n  typo_port: 3306\n',
                encoding='utf-8',
            )
            with self.assertRaisesRegex(ValueError, 'typo_port'):
                load_cfg(path)

    def test_connection_url_escapes_credentials(self):
        cfg = SimpleNamespace(
            user='a@b',
            password='p:/?#',
            host='localhost',
            port=3306,
            database='demo',
            db_type='mysql',
        )
        url = _build_conn_str(cfg)
        self.assertIn('a%40b', url)
        self.assertIn('p%3A%2F%3F%23', url)

    def test_postgresql_ssl_defaults_to_identity_verification(self):
        cfg = SimpleNamespace(
            db_type='postgresql',
            use_ssl=True,
            ssl_verify_cert=True,
            ssl_verify_identity=True,
            ssl_ca='/tmp/root.pem',
        )
        self.assertEqual(
            {'sslmode': 'verify-full', 'sslrootcert': '/tmp/root.pem'},
            get_connect_args(cfg),
        )

    def test_manage_table_quotes_postgresql_table(self):
        cfg = SimpleNamespace(
            name='pg',
            user='u',
            password='p',
            host='localhost',
            database='d',
            db_type='postgresql',
        )
        table = ManageTable('public.orders', cfg)
        with patch('tablemaster.database.opt') as execute:
            table.delete_table()
        self.assertEqual('DROP TABLE "public"."orders"', str(execute.call_args.args[0]))

    def test_delete_clause_cannot_be_empty(self):
        cfg = SimpleNamespace(
            name='db',
            user='u',
            password='p',
            host='localhost',
            database='d',
            db_type='mysql',
        )
        with self.assertRaisesRegex(ValueError, 'must not be empty'):
            ManageTable('orders', cfg).par_del(' ')

    def test_upsert_rejects_missing_key_before_connecting(self):
        cfg = SimpleNamespace(
            name='pg',
            user='u',
            password='p',
            host='localhost',
            database='d',
            db_type='postgresql',
        )
        with patch('tablemaster.database._resolve_engine') as resolve:
            with self.assertRaisesRegex(ValueError, 'not found'):
                ManageTable('orders', cfg).upsert_data(
                    pd.DataFrame({'id': [1]}),
                    key='missing',
                )
        resolve.assert_not_called()

    def test_upload_uses_qualified_table_schema(self):
        cfg = SimpleNamespace(
            name='pg',
            user='u',
            password='p',
            host='localhost',
            database='d',
            db_type='postgresql',
        )
        frame = pd.DataFrame({'id': [1]})
        with patch('tablemaster.database._resolve_engine', return_value=_Engine()):
            with patch.object(pd.DataFrame, 'to_sql') as to_sql:
                ManageTable('public.orders', cfg).upload_data(frame)
        self.assertEqual('orders', to_sql.call_args.kwargs['name'])
        self.assertEqual('public', to_sql.call_args.kwargs['schema'])
