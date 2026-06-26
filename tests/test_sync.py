from dataclasses import dataclass, field
from unittest import TestCase

import pandas as pd

from tablemaster.sync import SyncError, _merge_bidirectional, sync


@dataclass
class MemoryEndpoint:
    name: str
    frame: pd.DataFrame
    fail_write: bool = False
    writes: list[pd.DataFrame] = field(default_factory=list)

    @property
    def label(self):
        return self.name

    def read(self):
        return self.frame.copy()

    def write(self, df, *, key, on_conflict):
        if self.fail_write:
            raise RuntimeError('write failed')
        self.writes.append(df.copy())


class SyncTests(TestCase):
    def test_source_wins_is_row_level_and_fills_missing_columns(self):
        source = pd.DataFrame({'id': [1], 'name': ['source']})
        target = pd.DataFrame({'id': [1], 'name': ['target'], 'region': ['US']})
        merged = _merge_bidirectional(source, target, 'id')
        self.assertEqual(
            [{'id': 1, 'name': 'source', 'region': 'US'}],
            merged.to_dict(orient='records'),
        )

    def test_newest_policy_uses_updated_at(self):
        source = pd.DataFrame({'id': [1], 'name': ['old'], 'updated_at': ['2026-01-01']})
        target = pd.DataFrame({'id': [1], 'name': ['new'], 'updated_at': ['2026-02-01']})
        merged = _merge_bidirectional(
            source,
            target,
            'id',
            conflict_policy='newest',
            updated_at='updated_at',
        )
        self.assertEqual('new', merged.iloc[0]['name'])

    def test_sync_reports_partial_write(self):
        source = MemoryEndpoint('source', pd.DataFrame({'id': [1], 'name': ['a']}), fail_write=True)
        target = MemoryEndpoint('target', pd.DataFrame({'id': [2], 'name': ['b']}))
        with self.assertRaises(SyncError) as raised:
            sync(source, target, key='id')
        self.assertEqual(['target'], raised.exception.completed_endpoints)
        self.assertEqual('source', raised.exception.failed_endpoint)
        self.assertEqual(1, len(target.writes))

    def test_sync_rejects_unsupported_delete_semantics(self):
        endpoint = MemoryEndpoint('one', pd.DataFrame({'id': [1]}))
        with self.assertRaisesRegex(ValueError, 'tombstones'):
            sync(endpoint, endpoint, delete_policy='source_wins')
