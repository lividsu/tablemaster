from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import numpy as np
import pandas as pd

from tablemaster.local import batch_read, read
from tablemaster.serialization import dataframe_to_bitable_records, dataframe_to_sheet_values


class SerializationAndLocalTests(TestCase):
    def test_sheet_values_are_json_safe(self):
        frame = pd.DataFrame(
            {
                'number': [np.int64(2)],
                'missing': [np.nan],
                'when': [datetime(2026, 1, 2, 3, 4, 5)],
            }
        )
        values = dataframe_to_sheet_values(frame)
        self.assertEqual(['number', 'missing', 'when'], values[0])
        self.assertEqual([2, '', '2026-01-02T03:04:05'], values[1])

    def test_bitable_records_keep_dataframe_column_order(self):
        frame = pd.DataFrame({'b': [2], 'a': [1], 'ignored': [3]})
        records, skipped = dataframe_to_bitable_records(frame, {'a', 'b'})
        self.assertEqual(['b', 'a'], list(records[0]['fields']))
        self.assertEqual(['ignored'], skipped)

    def test_local_read_and_batch_read(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            (root / 'one.csv').write_text('id,name\n1,a\n', encoding='utf-8')
            (root / 'two.csv').write_text('id,name\n2,b\n', encoding='utf-8')
            single = read(root / 'one.csv', det_header=False)
            combined = batch_read(root / '*.csv', det_header=False)
            self.assertEqual([1], single['id'].tolist())
            self.assertEqual([1, 2], combined['id'].tolist())

    def test_batch_read_empty_pattern_has_clear_error(self):
        with TemporaryDirectory() as td:
            with self.assertRaises(FileNotFoundError):
                batch_read(Path(td) / '*.csv')
