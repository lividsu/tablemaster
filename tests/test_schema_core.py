from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from tablemaster.schema.dialects.mysql import MySQLDialect
from tablemaster.schema.diff import generate_plan
from tablemaster.schema.loader import load_schema_definitions
from tablemaster.schema.models import ActualColumn, ActualTable
from tablemaster.schema.pull import write_pulled_schema


class SchemaCoreTests(unittest.TestCase):
    def test_loader_reads_yaml(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            schema_dir = root / 'schema' / 'mydb' / 'ods'
            schema_dir.mkdir(parents=True, exist_ok=True)
            (schema_dir / 'orders.yaml').write_text(
                '\n'.join(
                    [
                        'table: orders',
                        'columns:',
                        '  - name: id',
                        '    type: BIGINT',
                        '    primary_key: true',
                        '    nullable: false',
                    ]
                ),
                encoding='utf-8',
            )
            tables = load_schema_definitions(connection='mydb', root_dir=root / 'schema')
            self.assertEqual(1, len(tables))
            self.assertEqual('orders', tables[0].table)
            self.assertEqual('id', tables[0].columns[0].name)

    def test_diff_emits_add_column_and_warning(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            schema_dir = root / 'schema' / 'mydb'
            schema_dir.mkdir(parents=True, exist_ok=True)
            (schema_dir / 'orders.yaml').write_text(
                '\n'.join(
                    [
                        'table: orders',
                        'columns:',
                        '  - name: id',
                        '    type: BIGINT',
                        '    nullable: false',
                        '  - name: amount',
                        '    type: DECIMAL(12,2)',
                    ]
                ),
                encoding='utf-8',
            )
            desired = load_schema_definitions(connection='mydb', root_dir=root / 'schema')
            actual = [
                ActualTable(
                    table='orders',
                    columns=[
                        ActualColumn(
                            name='id',
                            type='BIGINT',
                            nullable=False,
                            default=None,
                            comment=None,
                        ),
                        ActualColumn(
                            name='legacy_col',
                            type='VARCHAR(32)',
                            nullable=True,
                            default=None,
                            comment=None,
                        ),
                    ],
                    indexes=[],
                )
            ]
            plan = generate_plan('mydb', desired, actual, MySQLDialect())
            self.assertTrue(any(a.action == 'ADD_COLUMN' and a.column == 'amount' for a in plan.actions))
            self.assertTrue(any(w.action == 'COLUMN_MISSING' and w.column == 'legacy_col' for w in plan.warnings))

    def test_pull_writes_yaml(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            tables = [
                ActualTable(
                    table='orders',
                    columns=[
                        ActualColumn(
                            name='id',
                            type='BIGINT',
                            nullable=False,
                            default=None,
                            comment='主键',
                            primary_key=True,
                        )
                    ],
                    indexes=[],
                    comment='订单表',
                )
            ]
            paths = write_pulled_schema(tables, root / 'schema' / 'mydb')
            self.assertEqual(1, len(paths))
            content = paths[0].read_text(encoding='utf-8')
            self.assertIn('table: orders', content)
            self.assertIn('primary_key: true', content)


if __name__ == '__main__':
    unittest.main()
