# tablemaster

`tablemaster` is a Python toolkit for moving and managing tabular data across databases, Feishu/Lark, Google Sheets, and local files with one consistent API.

## Why tablemaster

- Unified DataFrame-first API across multiple data backends
- Production-friendly DB helpers (query, execute, chunked upload, upsert)
- Built-in Feishu and Google Sheets connectors
- Local CSV/Excel ingestion utilities
- Declarative two-way sync between Feishu Sheet and database table
- Schema-as-Code workflow (`init → schema plan → schema apply`)
- Configuration-first design for reproducible automation

## Installation

Install core package:

```bash
pip install -U tablemaster
```

Install backend-specific extras as needed:

```bash
pip install -U "tablemaster[database]" # Database support (MySQL/TiDB)
pip install -U "tablemaster[feishu]"   # Feishu/Lark connectors
pip install -U "tablemaster[gspread]"  # Google Sheets connectors
pip install -U "tablemaster[local]"    # Local CSV/Excel helpers
pip install -U "tablemaster[schema]"   # Schema management + SQLAlchemy + MySQL/PostgreSQL DB drivers
pip install -U "tablemaster[all]"      # Everything above
```

## Configuration

Load configuration with:

```python
import tablemaster as tm

cfg = tm.load_cfg()
```

Load config from another path:

```python
import os
import tablemaster as tm

cfg = tm.load_cfg(path="C:/configs/tablemaster/prod.yaml")
cfg = tm.load_cfg(path="C:/configs/tablemaster")

os.environ["TM_CFG_PATH"] = "D:/ops/tablemaster/cfg.yaml"
cfg = tm.load_cfg()
```

`load_cfg()` resolves config file in this order:

1. Explicit `path` argument
2. `TM_CFG_PATH` environment variable
3. `./cfg.yaml`
4. `~/.tablemaster/cfg.yaml`

Example `cfg.yaml`:

```yaml
mydb:
  host: 10.0.0.1
  user: admin
  password: secret
  database: bake_prod
  port: 3306
  db_type: mysql

db_tidb:
  host: sh.internal
  user: reader
  password: xxx
  database: analytics
  db_type: tidb
  use_ssl: true
  ssl_ca: /path/to/ca.pem

db_pg:
  host: 10.0.0.2
  user: postgres
  password: secret
  database: analytics
  port: 5432
  db_type: postgresql

feishu_prod:
  feishu_app_id: cli_xxx
  feishu_app_secret: yyy

gsheet:
  service_account_path: /absolute/path/to/service_account.json
```

For Google Sheets authentication setup, see:
<https://docs.gspread.org/en/latest/oauth2.html>

## Quick Start

### Query and execute SQL

```python
import tablemaster as tm

cfg = tm.load_cfg()
df = tm.query("SELECT * FROM orders LIMIT 20", cfg.mydb)
tm.opt("ALTER TABLE orders RENAME COLUMN old_col TO new_col", cfg.mydb)
```

### Manage database tables

```python
import tablemaster as tm

cfg = tm.load_cfg()
tb = tm.ManageTable("orders", cfg.mydb)
tb.upload_data(df, add_date=True)
tb.upsert_data(df, key="order_id")
tb.par_del("order_date > '2023-01-01'")
```

### Google Sheets

```python
import tablemaster as tm

cfg = tm.load_cfg()
sheet = ("spreadsheet_id_or_name", "worksheet_name")
df = tm.gs_read_df(sheet, cfg.gsheet)
tm.gs_write_df(sheet, df, cfg.gsheet)
```

### Feishu / Lark

```python
import tablemaster as tm

cfg = tm.load_cfg()
feishu_sheet = ("spreadsheet_token", "sheet_id")
feishu_base = ("app_token", "table_id")

sheet_df = tm.fs_read_df(feishu_sheet, cfg.feishu_prod)
base_df = tm.fs_read_base(feishu_base, cfg.feishu_prod)
tm.fs_write_df(feishu_sheet, sheet_df, cfg.feishu_prod, loc="A1", clear_sheet=False)
tm.fs_write_base(feishu_base, base_df, cfg.feishu_prod, clear_table=False)
```

### Local files

```python
import tablemaster as tm

single_df = tm.read("*orders_2026*")
merged_df = tm.batch_read("*orders_2026*")
df_list = tm.read_dfs("*orders_2026*")
```

### Declarative two-way sync

```python
import tablemaster as tm

cfg = tm.load_cfg()
feishu_sheet = ("spreadsheet_token", "sheet_id")

merged = tm.sync(
    source=("feishu", feishu_sheet, cfg.feishu_prod),
    target=("db", cfg.mydb, "orders"),
    on_conflict="upsert",
    key="order_id",
)
```

### Schema management (YAML-driven)

```bash
tablemaster init
tablemaster schema plan mydb --cfg-path ./cfg.yaml
tablemaster schema apply mydb --cfg-path ./cfg.yaml
tablemaster schema pull mydb --cfg-path ./cfg.yaml
```

Example schema file:

```yaml
table: ods_orders
columns:
  - name: id
    type: BIGINT
    primary_key: true
    nullable: false
  - name: order_no
    type: VARCHAR(64)
    nullable: false
indexes:
  - name: idx_order_no
    columns: [order_no]
    unique: true
```

## CLI

`tablemaster` now ships with a built-in CLI:

```bash
tablemaster --help
python -m tablemaster --help
```

Commands:

```bash
tablemaster version-info
tablemaster config list --cfg-path ./cfg.yaml
tablemaster config show mydb --cfg-path ./cfg.yaml
tablemaster db query "SELECT * FROM orders LIMIT 20" --cfg-key mydb --cfg-path ./cfg.yaml
tablemaster db query "SELECT * FROM orders" --cfg-key mydb --output ./out/orders.csv
tablemaster local read "*orders_2026*" --limit 10
tablemaster local read "*orders_2026*" --no-det-header
tablemaster init --cfg-path ./cfg.yaml
tablemaster schema plan mydb --cfg-path ./cfg.yaml --output ./plan.json
tablemaster schema apply mydb --cfg-path ./cfg.yaml --plan-file ./plan.json --auto-approve
tablemaster schema pull mydb --cfg-path ./cfg.yaml --output-dir ./schema
```

CLI command groups:

- `version-info`: Print installed package version.
- <br />
- `config show <cfg_key>`: Print one config entry as JSON.
- `db query <sql>`: Run SQL with `--cfg-key`; use `--limit` to control stdout preview and `--output` to export full result as CSV.
- `local read <pattern>`: Read one local CSV/Excel match and print preview; use `--det-header/--no-det-header` to control header detection.
- `config list`: List top-level keys from config.
- `init`: Bootstrap `cfg.yaml` and `schema/<connection>/` scaffold in current directory.
- `schema plan <connection>`: Compare YAML schema and live DB, print/apply-safe plan.
- `schema apply <connection>`: Execute DDL actions from generated or saved plan.
- `schema pull <connection>`: Generate YAML schema files from live DB tables.

`--cfg-path` accepts either a config file path or a directory containing `cfg.yaml`.

## Public API

- Database: `query`, `opt`, `ManageTable`
- Feishu/Lark: `fs_read_df`, `fs_write_df`, `fs_read_base`, `fs_write_base`
- Google Sheets: `gs_read_df`, `gs_write_df`
- Local files: `read`, `batch_read`, `read_dfs`
- Sync: `sync`
- Config: `load_cfg`
- Schema: `load_schema_definitions`, `introspect_tables`, `generate_plan`, `render_plan`, `save_plan`, `load_plan`, `apply_plan`, `init_scaffold`, `pull_schema`, `write_pulled_schema`

## Notes

- Python 3.9+ is required.
- CLI entrypoint is `tablemaster`; use `tablemaster --help` for command details.
- `tm.cfg` and `read_cfg()` are backward-compatible but deprecated in favor of `load_cfg()`.
- `tablemaster[database]` is the recommended DB extra name; `tablemaster[mysql]` remains available for compatibility.
- PostgreSQL is supported (`db_type: postgresql`) for query/execute and upsert (`ON CONFLICT`).
- For PostgreSQL dependencies, use `tablemaster[schema]` or install `SQLAlchemy` + `psycopg2-binary` manually.
