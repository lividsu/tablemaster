from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from tablemaster.database import ManageTable


class _BeginContext:
    def __enter__(self):
        return SimpleNamespace()

    def __exit__(self, exc_type, exc, tb):
        return False


class _Engine:
    def begin(self):
        return _BeginContext()


def _postgresql_cfg():
    return SimpleNamespace(
        name='pg',
        user='u',
        password='p',
        host='127.0.0.1',
        database='d',
        db_type='postgresql',
    )


class PostgreSQLUpsertTests(TestCase):
    def test_postgresql_upsert_uses_execute_values(self):
        table = ManageTable('orders', _postgresql_cfg())
        df = pd.DataFrame({'id': [1, 2], 'name': ['alpha', None]})

        with patch('tablemaster.database._resolve_engine', return_value=_Engine()):
            with patch('tablemaster.database._execute_postgresql_values') as execute_values:
                table.upsert_data(df, key='id')

        execute_values.assert_called_once()
        _, sql, rows = execute_values.call_args.args
        self.assertIn('INSERT INTO orders ("id", "name")', sql)
        self.assertIn('VALUES %s', sql)
        self.assertIn('ON CONFLICT ("id") DO UPDATE SET "name"=EXCLUDED."name"', sql)
        self.assertEqual([(1, 'alpha'), (2, None)], rows)
        self.assertEqual(10000, execute_values.call_args.kwargs['page_size'])

    def test_postgresql_ignore_uses_on_conflict_do_nothing(self):
        table = ManageTable('public.orders', _postgresql_cfg())
        df = pd.DataFrame({'id': [1], 'name': ['alpha']})

        with patch('tablemaster.database._resolve_engine', return_value=_Engine()):
            with patch('tablemaster.database._execute_postgresql_values') as execute_values:
                table.upsert_data(df, ignore=True)

        execute_values.assert_called_once()
        _, sql, rows = execute_values.call_args.args
        self.assertIn('INSERT INTO public.orders ("id", "name")', sql)
        self.assertIn('ON CONFLICT DO NOTHING', sql)
        self.assertEqual([(1, 'alpha')], rows)
