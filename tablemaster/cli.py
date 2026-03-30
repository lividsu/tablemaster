import json
from dataclasses import asdict, is_dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Optional

import typer

from .config import load_cfg

app = typer.Typer(help='CLI for tablemaster data operations.')
config_app = typer.Typer(help='Inspect config entries.')
db_app = typer.Typer(help='Run database operations.')
local_app = typer.Typer(help='Read local files.')


def _to_plain(value):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain(v) for v in value]
    if hasattr(value, '__dict__'):
        return {k: _to_plain(v) for k, v in vars(value).items()}
    return value


def _load_named_cfg(cfg_path: Optional[str], cfg_key: str):
    cfg = load_cfg(cfg_path)
    if not hasattr(cfg, cfg_key):
        raise typer.BadParameter(f'Config key not found: {cfg_key}')
    return getattr(cfg, cfg_key)


@app.command()
def version_info():
    try:
        pkg_version = version('tablemaster')
    except PackageNotFoundError:
        pkg_version = 'dev'
    typer.echo(pkg_version)


@config_app.command('list')
def config_list(
    cfg_path: Optional[str] = typer.Option(None, '--cfg-path', help='Config file path or directory.'),
):
    cfg = load_cfg(cfg_path)
    keys = sorted(vars(cfg).keys())
    typer.echo('\n'.join(keys))


@config_app.command('show')
def config_show(
    cfg_key: str = typer.Argument(..., help='Top-level config key.'),
    cfg_path: Optional[str] = typer.Option(None, '--cfg-path', help='Config file path or directory.'),
):
    entry = _load_named_cfg(cfg_path, cfg_key)
    typer.echo(json.dumps(_to_plain(entry), ensure_ascii=False, indent=2))


@db_app.command('query')
def db_query(
    sql: str = typer.Argument(..., help='SQL to execute.'),
    cfg_key: str = typer.Option(..., '--cfg-key', help='Database config key in cfg.yaml.'),
    cfg_path: Optional[str] = typer.Option(None, '--cfg-path', help='Config file path or directory.'),
    output: Optional[Path] = typer.Option(None, '--output', help='Optional CSV output path.'),
    limit: int = typer.Option(100, '--limit', min=1, help='Max rows to print to stdout.'),
):
    from .database import query

    db_cfg = _load_named_cfg(cfg_path, cfg_key)
    df = query(sql, db_cfg)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output, index=False)
        typer.echo(f'Wrote {len(df)} rows to {output}')
    preview = df.head(limit)
    typer.echo(preview.to_csv(index=False))


@local_app.command('read')
def local_read(
    pattern: str = typer.Argument(..., help='Glob pattern to read, e.g. "*orders_2026*".'),
    det_header: bool = typer.Option(True, '--det-header/--no-det-header', help='Enable header detection.'),
    limit: int = typer.Option(20, '--limit', min=1, help='Max rows to print.'),
):
    from .local import read

    df = read(pattern, det_header=det_header)
    typer.echo(df.head(limit).to_csv(index=False))


app.add_typer(config_app, name='config')
app.add_typer(db_app, name='db')
app.add_typer(local_app, name='local')
