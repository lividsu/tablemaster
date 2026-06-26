from __future__ import annotations

import glob
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _detect_header(reader, path: Path, det_rows: int = 10) -> pd.DataFrame:
    first = reader(path)
    if sum(str(column).startswith('Unnamed') for column in first.columns) <= 1:
        return first
    for header_row in range(1, det_rows + 1):
        candidate = reader(path, header=header_row)
        if not any(str(column).startswith('Unnamed') for column in candidate.columns):
            return candidate
    return first


def detect_header_read_csv(path, det_rows=10):
    return _detect_header(pd.read_csv, Path(path), det_rows=det_rows)


def detect_header_read_excel(path, det_rows=10):
    return _detect_header(pd.read_excel, Path(path), det_rows=det_rows)


def equal_table(df1, df2, det_col='nan'):
    if len(df1) != len(df2):
        return False
    if df1.equals(df2):
        return True
    if det_col == 'nan':
        return False
    if det_col not in df1.columns or det_col not in df2.columns:
        raise ValueError(f'deduplication column not found in both DataFrames: {det_col}')
    left = df1[det_col].fillna('').sort_values().reset_index(drop=True)
    right = df2[det_col].fillna('').sort_values().reset_index(drop=True)
    return left.equals(right)


def _matched_files(pattern) -> list[Path]:
    matches = sorted(Path(match) for match in glob.glob(str(pattern), recursive=True))
    return [
        path
        for path in matches
        if path.is_file() and not any(part.startswith('.') for part in path.parts if part not in {'.', '..'})
    ]


def _read_path(path: Path, det_header: bool) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {'.xlsx', '.xls', '.xlsm', '.xlsb'}:
        return detect_header_read_excel(path) if det_header else pd.read_excel(path)
    if suffix in {'.csv', '.txt'}:
        return detect_header_read_csv(path) if det_header else pd.read_csv(path)
    if suffix == '.parquet':
        return pd.read_parquet(path)
    raise ValueError(f'unsupported file type: {suffix or "<none>"}')


def read(file, det_header=True):
    matches = _matched_files(file)
    if len(matches) > 1:
        raise ValueError(f'More than one file matched; specify one file: {matches}')
    if not matches:
        raise FileNotFoundError(f'No file matched: {file}')
    return _read_path(matches[0], det_header=det_header)


def read_dfs(file, det_col='nan', det_header=True):
    matches = _matched_files(file)
    if not matches:
        raise FileNotFoundError(f'No file matched: {file}')
    logger.info('found %s files: %s', len(matches), matches)
    unique: list[pd.DataFrame] = []
    for path in matches:
        frame = _read_path(path, det_header=det_header)
        if not any(equal_table(frame, existing, det_col) for existing in unique):
            unique.append(frame)
    logger.info('found %s unique files', len(unique))
    return unique


def batch_read(file, det_col='nan', det_header=True):
    frames = read_dfs(file, det_col=det_col, det_header=det_header)
    return pd.concat(frames, ignore_index=True)
