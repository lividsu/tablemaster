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
schema_app = typer.Typer(help='Manage database schema (plan/apply/pull).')


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


def _build_plan(
    connection: str,
    cfg_path: Optional[str],
    schema_dir: Path,
    table: Optional[str] = None,
):
    from .schema.dialects import get_dialect
    from .schema.diff import generate_plan
    from .schema.introspect import introspect_tables
    from .schema.loader import load_schema_definitions

    db_cfg = _load_named_cfg(cfg_path, connection)
    desired = load_schema_definitions(connection=connection, root_dir=schema_dir, table=table)
    actual = introspect_tables(
        db_cfg,
        table_names=[table] if table else None,
        schema_name=getattr(desired[0], 'schema_name', None) if desired else None,
    )
    dialect = get_dialect(getattr(db_cfg, 'db_type', 'mysql'))
    plan = generate_plan(connection_name=connection, desired=desired, actual=actual, dialect=dialect)
    return db_cfg, plan


@app.command('init')
def init_project(
    cfg_path: Optional[str] = typer.Option(None, '--cfg-path', help='Config file path.'),
    connection: Optional[list[str]] = typer.Option(
        None,
        '--connection',
        '-c',
        help='Only initialize selected connections. Repeat option for multiple values.',
    ),
):
    from .schema.init import init_scaffold

    result = init_scaffold(
        base_dir=Path('.').resolve(),
        cfg_path=cfg_path,
        connections=connection,
    )
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


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


@schema_app.command('plan')
def schema_plan(
    connection: str = typer.Argument(..., help='Database config key in cfg.yaml.'),
    table: Optional[str] = typer.Option(None, '--table', help='Only plan one table.'),
    cfg_path: Optional[str] = typer.Option(None, '--cfg-path', help='Config file path or directory.'),
    schema_dir: Path = typer.Option(Path('schema'), '--schema-dir', help='Schema root directory.'),
    output: Optional[Path] = typer.Option(None, '--output', help='Write plan JSON to file.'),
):
    from .schema.plan import render_plan, save_plan

    db_cfg, plan = _build_plan(connection, cfg_path, schema_dir, table=table)
    conn_info = f"{db_cfg.db_type}@{db_cfg.host}:{db_cfg.port}/{db_cfg.database}"
    typer.echo(render_plan(plan, connection_info=conn_info))
    if output:
        saved = save_plan(plan, output)
        typer.echo(f'Wrote plan to {saved}')


@schema_app.command('apply')
def schema_apply(
    connection: str = typer.Argument(..., help='Database config key in cfg.yaml.'),
    auto_approve: bool = typer.Option(False, '--auto-approve', help='Execute without confirmation prompts.'),
    plan_file: Optional[Path] = typer.Option(None, '--plan-file', '--plan', help='Apply an existing plan JSON.'),
    table: Optional[str] = typer.Option(None, '--table', help='Only apply one table when plan is generated online.'),
    cfg_path: Optional[str] = typer.Option(None, '--cfg-path', help='Config file path or directory.'),
    schema_dir: Path = typer.Option(Path('schema'), '--schema-dir', help='Schema root directory.'),
):
    from .schema.apply import apply_plan
    from .schema.plan import load_plan, render_plan

    db_cfg = _load_named_cfg(cfg_path, connection)
    if plan_file:
        plan = load_plan(plan_file)
    else:
        _, plan = _build_plan(connection, cfg_path, schema_dir, table=table)
    conn_info = f"{db_cfg.db_type}@{db_cfg.host}:{db_cfg.port}/{db_cfg.database}"
    typer.echo(render_plan(plan, connection_info=conn_info))
    result = apply_plan(plan, db_cfg, auto_approve=auto_approve)
    typer.echo(f'Apply summary: {result.summary()}')


@schema_app.command('pull')
def schema_pull(
    connection: str = typer.Argument(..., help='Database config key in cfg.yaml.'),
    table: Optional[str] = typer.Option(None, '--table', help='Only pull one table.'),
    cfg_path: Optional[str] = typer.Option(None, '--cfg-path', help='Config file path or directory.'),
    output_dir: Path = typer.Option(Path('schema'), '--output-dir', help='Schema output root directory.'),
    schema_name: Optional[str] = typer.Option(None, '--schema-name', help='Database schema namespace, e.g. public.'),
):
    from .schema.introspect import introspect_tables
    from .schema.pull import pull_schema

    db_cfg = _load_named_cfg(cfg_path, connection)
    paths = pull_schema(
        cfg=db_cfg,
        introspect_func=introspect_tables,
        connection=connection,
        output_dir=output_dir,
        table=table,
        schema_name=schema_name,
    )
    typer.echo('\n'.join(str(p) for p in paths))


app.add_typer(config_app, name='config')
app.add_typer(db_app, name='db')
app.add_typer(local_app, name='local')
app.add_typer(schema_app, name='schema')
