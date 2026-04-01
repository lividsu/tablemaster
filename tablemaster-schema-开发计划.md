# Tablemaster Schema 管理功能 — 开发计划

## 一、背景与目标

tablemaster 现有能力覆盖了"读/写数据"（MySQL、PostgreSQL、TiDB、Feishu、Google Sheet、本地文件），但缺少**表结构生命周期管理**。本次迭代的目标是引入类似 Terraform 的 `init → plan → apply` 工作流，让用户可以：

1. 用 YAML 声明式地定义数据库表结构（Schema-as-Code）
2. 自动对比声明与线上实际差异，生成变更计划
3. 安全地执行计划，并对破坏性操作（删列/删表）仅发出警告

---

## 二、整体架构

```
tablemaster/
├── tablemaster/
│   ├── __init__.py          # 现有
│   ├── cli.py               # 扩展: schema_app 子命令组
│   ├── config.py            # 现有
│   ├── database.py          # 现有 (复用 _resolve_engine / query / opt)
│   ├── schema/              # 【新增模块】
│   │   ├── __init__.py
│   │   ├── models.py        # Schema YAML 的 dataclass / Pydantic 模型
│   │   ├── loader.py        # 读取 schema/ 目录 → 内存模型
│   │   ├── introspect.py    # 连接数据库 → 获取实际结构
│   │   ├── diff.py          # 对比 desired vs actual → Plan
│   │   ├── plan.py          # Plan 对象: 序列化 / 展示 / 持久化
│   │   ├── apply.py         # 执行 Plan → 生成并运行 DDL
│   │   ├── dialects/        # 各数据库 DDL 方言
│   │   │   ├── __init__.py
│   │   │   ├── base.py      # 抽象基类
│   │   │   ├── mysql.py
│   │   │   ├── postgresql.py
│   │   │   └── tidb.py      # 继承 mysql, 覆盖差异点
│   │   └── init.py          # tablemaster init 脚手架逻辑
│   └── ...
├── example/
│   ├── cfg.yaml             # 现有
│   └── schema/              # 示例 schema 目录
│       └── mydb/
│           └── ods/
│               └── ods_orders.yaml
└── pyproject.toml           # 新增 optional-dep: schema
```

---

## 三、目录约定 & 初始化

### 3.1 `tablemaster init`

用户在任意路径执行 `tablemaster init`，生成以下脚手架：

```
./
├── cfg.yaml                          # 连接配置（若不存在则新建模板）
└── schema/                           # 表结构定义根目录
    └── <cfg_key>/                    # 与 cfg.yaml 中的连接名一一对应
        └── <schema_or_layer>/        # 用户自定义分层 (ods / dwd / ads …)
            └── .gitkeep
```

**规则**：

- `schema/` 下的一级子目录名 **必须** 匹配 `cfg.yaml` 中的某个 `DBConfig` key（如 `pg_main`、`pg_warehouse`）。这保证每张表都能找到对应的连接。
- 二级子目录是可选的逻辑分层，不影响运行逻辑，仅用于组织。
- 每个 `.yaml` 文件定义一张表。

### 3.2 CLI 交互

```bash
tablemaster init                         # 当前目录，根据 cfg.yaml 自动生成
tablemaster init --cfg-path /path/to/cfg # 指定配置文件
tablemaster init --connections pg_main   # 只为指定连接生成目录
```

`init` 应当是幂等的——已存在的文件/目录不覆盖，仅补全缺失项。

---

## 四、Schema YAML 定义格式

### 4.1 单表定义（如 `schema/pg_main/ods/ods_orders.yaml`）

```yaml
table: ods_orders
# database: 可选，覆盖 cfg 中的 database
# schema: public  # PostgreSQL schema，默认 public
comment: "原始订单表"

columns:
  - name: id
    type: BIGINT
    primary_key: true
    nullable: false
    comment: "主键"

  - name: order_no
    type: VARCHAR(64)
    nullable: false
    comment: "订单编号"

  - name: user_id
    type: BIGINT
    nullable: true
    comment: "用户ID"

  - name: amount
    type: DECIMAL(12,2)
    default: "0.00"
    comment: "金额"

  - name: status
    type: SMALLINT
    default: "0"
    comment: "状态: 0-待支付, 1-已支付, 2-已取消"

  - name: created_at
    type: TIMESTAMP
    nullable: false
    default: "CURRENT_TIMESTAMP"
    comment: "创建时间"

  - name: updated_at
    type: TIMESTAMP
    nullable: true
    comment: "更新时间"

indexes:
  - name: idx_order_no
    columns: [order_no]
    unique: true

  - name: idx_user_id
    columns: [user_id]

  - name: idx_created_status
    columns: [created_at, status]
```

### 4.2 类型映射策略

YAML 中的 `type` 使用**通用类型名**，在生成 DDL 时由 dialect 层做映射：

| YAML 中写的类型 | MySQL / TiDB | PostgreSQL |
|---|---|---|
| `BIGINT` | `BIGINT` | `BIGINT` |
| `VARCHAR(n)` | `VARCHAR(n)` | `VARCHAR(n)` |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | `NUMERIC(p,s)` |
| `TIMESTAMP` | `DATETIME` | `TIMESTAMP` |
| `TEXT` | `TEXT` | `TEXT` |
| `BOOLEAN` | `TINYINT(1)` | `BOOLEAN` |
| `JSON` | `JSON` | `JSONB` |

也允许用户直接写原生类型（如 `JSONB`），此时 dialect 层不做转换，直接透传。

---

## 五、核心流程设计

### 5.1 `tablemaster plan <connection>`

```bash
tablemaster plan pg_main               # 对比 schema/pg_main/ 下所有表
tablemaster plan pg_main --table ods_orders  # 只对比单表
tablemaster plan pg_main --output plan.json  # 输出 plan 到文件
```

**执行步骤**：

```
1. 加载 cfg.yaml → 拿到 pg_main 的 DBConfig
2. 加载 schema/pg_main/**/*.yaml → 解析为 DesiredTable 列表
3. 连接数据库 → introspect 获取 ActualTable 列表
4. diff(desired, actual) → 生成 Plan
5. 打印 Plan 到终端 / 输出到文件
```

### 5.2 Plan 的操作类型

| 操作 | 符号 | 说明 | 是否自动执行 |
|---|---|---|---|
| 创建表 | `+ CREATE TABLE` | 表在 YAML 中存在，数据库中不存在 | ✅ |
| 新增列 | `+ ADD COLUMN` | 列在 YAML 中存在，数据库中不存在 | ✅ |
| 修改列类型 | `~ ALTER COLUMN TYPE` | 类型不一致 | ✅ |
| 修改列注释 | `~ ALTER COLUMN COMMENT` | 注释不一致 | ✅ |
| 修改 nullable | `~ ALTER COLUMN NULLABLE` | nullable 不一致 | ✅ |
| 修改 default | `~ ALTER COLUMN DEFAULT` | 默认值不一致 | ✅ |
| 创建索引 | `+ CREATE INDEX` | 索引在 YAML 中存在，数据库中不存在 | ✅ |
| 删除索引 | `- DROP INDEX` | 索引在数据库中存在，YAML 中不存在 | ✅ |
| **列消失** | `⚠ COLUMN MISSING` | 列在数据库中存在，YAML 中没有 | ❌ 仅警告 |
| **表消失** | `⚠ TABLE MISSING` | 表在数据库中存在，但无对应 YAML | ❌ 仅警告 |

### 5.3 Plan 输出示例

```
tablemaster plan pg_main

📋 Plan for [pg_main] (PostgreSQL @ 10.0.0.1:5432/warehouse)
─────────────────────────────────────────────────────────────

+ CREATE TABLE ods_orders
    columns: id (BIGINT PK), order_no (VARCHAR(64)), user_id (BIGINT),
             amount (NUMERIC(12,2)), status (SMALLINT), created_at (TIMESTAMP),
             updated_at (TIMESTAMP)
    indexes: idx_order_no (UNIQUE), idx_user_id, idx_created_status

~ ALTER TABLE ods_users
    ~ MODIFY COLUMN email: VARCHAR(100) → VARCHAR(255)
    ~ MODIFY COLUMN comment on phone: NULL → "手机号"
    + ADD COLUMN avatar_url VARCHAR(512) COMMENT "头像"
    + CREATE INDEX idx_email ON (email)

⚠ WARNING: Column ods_users.old_field exists in DB but NOT in schema YAML
⚠ WARNING: Table legacy_logs exists in DB but has no schema YAML definition

─────────────────────────────────────────────────────────────
Summary: 1 create, 1 alter (4 changes), 2 warnings
```

### 5.4 `tablemaster apply <connection>`

```bash
tablemaster apply pg_main                # 先 plan，再交互确认后执行
tablemaster apply pg_main --auto-approve # 跳过确认（CI/CD 场景）
tablemaster apply pg_main --plan plan.json  # 执行之前保存的 plan
```

**执行步骤**：

```
1. 如果没有传入 plan 文件：执行一次 plan 流程
2. 打印 plan 摘要
3. 如果有 ⚠ WARNING，单独高亮显示，提醒用户手动处理
4. 询问确认 (除非 --auto-approve)
5. 按顺序执行 DDL:
   a. CREATE TABLE (先建表，避免 FK 依赖问题暂不处理外键)
   b. ADD COLUMN
   c. ALTER COLUMN (type / nullable / default / comment)
   d. CREATE INDEX / DROP INDEX
6. 每条 DDL 执行后打印结果 (✓ / ✗)
7. 任何一条失败则暂停，询问是否继续
8. 最终汇总报告
```

---

## 六、模块详细设计

### 6.1 `schema/models.py` — 数据模型

```python
@dataclass
class ColumnDef:
    name: str
    type: str                    # 通用类型或原生类型
    primary_key: bool = False
    nullable: bool = True
    default: Optional[str] = None
    comment: Optional[str] = None

@dataclass
class IndexDef:
    name: str
    columns: List[str]
    unique: bool = False

@dataclass
class TableDef:
    table: str
    columns: List[ColumnDef]
    indexes: List[IndexDef] = field(default_factory=list)
    comment: Optional[str] = None
    database: Optional[str] = None    # 覆盖 cfg 中的 database
    schema_name: Optional[str] = None # PostgreSQL schema, 默认 public

@dataclass
class ActualColumn:
    """从数据库 introspect 得到的列信息"""
    name: str
    type: str          # 数据库返回的原生类型
    nullable: bool
    default: Optional[str]
    comment: Optional[str]

@dataclass
class ActualTable:
    table: str
    columns: List[ActualColumn]
    indexes: List[IndexDef]
    comment: Optional[str] = None
```

### 6.2 `schema/introspect.py` — 数据库反查

通过 `information_schema` 或 SQLAlchemy Inspector 获取线上结构：

- **MySQL / TiDB**：查询 `information_schema.COLUMNS`、`information_schema.STATISTICS`、`information_schema.TABLES`
- **PostgreSQL**：查询 `information_schema.columns`、`pg_indexes`、`pg_description`

关键设计：复用现有 `database._resolve_engine()`，不引入新的连接管理。

```python
def introspect_tables(cfg: DBConfig, table_names: List[str] = None) -> List[ActualTable]:
    """获取数据库中的实际表结构"""
    dialect = get_dialect(cfg.db_type)
    engine = _resolve_engine(cfg)
    with engine.connect() as conn:
        return dialect.introspect(conn, cfg.database, table_names)
```

### 6.3 `schema/diff.py` — 差异对比

```python
@dataclass
class PlanAction:
    action: str          # CREATE_TABLE / ADD_COLUMN / ALTER_COLUMN_TYPE / ...
    table: str
    column: Optional[str] = None
    detail: dict = field(default_factory=dict)  # old_type, new_type 等
    ddl: str = ""        # 生成好的 DDL 语句
    is_warning: bool = False   # 是否为仅警告不执行

@dataclass
class Plan:
    connection: str
    actions: List[PlanAction]
    warnings: List[PlanAction]
    created_at: str      # ISO 时间戳

    def has_changes(self) -> bool: ...
    def summary(self) -> str: ...
    def to_json(self) -> str: ...

    @classmethod
    def from_json(cls, data: str) -> "Plan": ...
```

核心 diff 逻辑：

```python
def generate_plan(
    connection_name: str,
    desired: List[TableDef],
    actual: List[ActualTable],
    dialect: BaseDialect
) -> Plan:
    # 1. desired 中有、actual 中没有 → CREATE TABLE
    # 2. 两边都有 → 逐列对比
    #    a. desired 有、actual 没有 → ADD COLUMN
    #    b. 两边都有 → 比较 type/nullable/default/comment → ALTER
    #    c. actual 有、desired 没有 → WARNING (列消失)
    # 3. 对比 indexes
    # 4. actual 中有、desired 中完全没有此表 → WARNING (表消失)
```

### 6.4 `schema/dialects/` — DDL 方言层

```python
# base.py
class BaseDialect(ABC):
    @abstractmethod
    def map_type(self, generic_type: str) -> str:
        """通用类型 → 数据库原生类型"""

    @abstractmethod
    def normalize_type(self, db_type: str) -> str:
        """数据库返回的类型 → 标准化字符串（用于比较）"""

    @abstractmethod
    def introspect(self, conn, database: str, tables: list) -> List[ActualTable]: ...

    @abstractmethod
    def gen_create_table(self, table: TableDef) -> str: ...

    @abstractmethod
    def gen_add_column(self, table: str, col: ColumnDef) -> str: ...

    @abstractmethod
    def gen_alter_column_type(self, table: str, col_name: str, new_type: str) -> str: ...

    @abstractmethod
    def gen_alter_column_comment(self, table: str, col_name: str, comment: str) -> str: ...

    @abstractmethod
    def gen_create_index(self, table: str, index: IndexDef) -> str: ...

    @abstractmethod
    def gen_drop_index(self, table: str, index_name: str) -> str: ...
```

各方言继承后实现具体 DDL。`tidb.py` 可以继承 `mysql.py`，仅覆盖差异点。

### 6.5 `schema/apply.py` — 执行引擎

```python
def apply_plan(plan: Plan, cfg: DBConfig, auto_approve: bool = False) -> ApplyResult:
    # 1. 过滤掉 is_warning 的 action
    # 2. 如果 !auto_approve → 打印 plan + 请求确认
    # 3. 逐条执行 DDL (使用 database.opt())
    # 4. 记录每条的成功/失败
    # 5. 失败时询问是否继续 (除非 auto_approve 模式下直接中止)
    # 6. 返回 ApplyResult 汇总
```

---

## 七、CLI 命令注册

在 `cli.py` 中新增 `schema_app` 子命令组：

```python
schema_app = typer.Typer(help="Manage database schema (init / plan / apply).")

# tablemaster init
@app.command()
def init(...): ...

# tablemaster plan <connection>
@schema_app.command("plan")
def schema_plan(
    connection: str,
    table: Optional[str] = None,
    cfg_path: Optional[str] = None,
    output: Optional[Path] = None,
): ...

# tablemaster apply <connection>
@schema_app.command("apply")
def schema_apply(
    connection: str,
    auto_approve: bool = False,
    plan_file: Optional[Path] = None,
    cfg_path: Optional[str] = None,
): ...

# tablemaster schema pull <connection>
@schema_app.command("pull")
def schema_pull(
    connection: str,
    cfg_path: Optional[str] = None,
    output_dir: Optional[Path] = None,
): ...
    """反向生成: 从数据库现有结构生成 YAML 文件 (方便存量项目接入)"""

app.add_typer(schema_app, name="schema")
```

最终 CLI 结构：

```
tablemaster
├── init                    # 初始化项目结构
├── version                 # 版本号
├── config
│   ├── list
│   └── show
├── db
│   └── query
├── local
│   └── read
└── schema
    ├── plan <connection>   # 对比并生成计划
    ├── apply <connection>  # 执行计划
    └── pull <connection>   # 从线上反向生成 YAML
```

---

## 八、`schema pull` — 存量项目接入

对于已有数据库的团队，手写所有表的 YAML 不现实。`schema pull` 反向生成：

```bash
tablemaster schema pull pg_main                    # 拉取所有表
tablemaster schema pull pg_main --table ods_orders # 只拉取指定表
tablemaster schema pull pg_main --output-dir ./schema/pg_main/raw/
```

生成的 YAML 放入 `schema/<connection>/` 下，用户可手动整理分层。

---

## 九、开发里程碑

### Phase 1：基础骨架（约 1 周）

| 任务 | 产出 |
|---|---|
| 定义 `models.py` 数据模型 | `TableDef`, `ColumnDef`, `IndexDef`, `PlanAction`, `Plan` |
| 实现 `loader.py` | 读取 `schema/` 目录，解析 YAML → `TableDef` 列表 |
| 实现 `init.py` | `tablemaster init` 脚手架生成 |
| 注册 CLI 命令 | `init` / `schema plan` / `schema apply` 入口（先空壳） |
| 补充 `pyproject.toml` | 新增 `schema` optional-dep（`pyyaml` 已有，可能需要 `pydantic`） |
| 单元测试 | YAML 加载、目录校验 |

### Phase 2：Introspect + Diff（约 1.5 周）

| 任务 | 产出 |
|---|---|
| 实现 `dialects/mysql.py` introspect | 从 MySQL/TiDB 获取表结构 |
| 实现 `dialects/postgresql.py` introspect | 从 PostgreSQL 获取表结构 |
| 实现 `diff.py` | desired vs actual 差异对比 → Plan |
| 实现 `plan.py` | Plan 终端格式化输出 + JSON 序列化 |
| 类型标准化 | `normalize_type()` 处理各种变体（`int(11)` vs `INT`, `character varying` vs `VARCHAR`） |
| 单元测试 | mock 数据库结构，验证 diff 正确性 |

### Phase 3：DDL 生成 + Apply（约 1 周）

| 任务 | 产出 |
|---|---|
| `dialects/mysql.py` DDL 生成 | CREATE TABLE / ADD COLUMN / ALTER / INDEX |
| `dialects/postgresql.py` DDL 生成 | 同上，适配 PG 语法 |
| `dialects/tidb.py` | 继承 MySQL，覆盖差异 |
| 实现 `apply.py` | 执行 Plan，带确认 + 错误处理 |
| 集成测试 | 用 Docker 启动 MySQL + PG，端到端验证 |

### Phase 4：Pull + 打磨（约 1 周）

| 任务 | 产出 |
|---|---|
| 实现 `schema pull` | 反向生成 YAML |
| 完善 WARNING 机制 | 列消失/表消失的提示文案和颜色 |
| 支持 `--dry-run` | apply 时只打印 DDL 不执行 |
| 文档 + README 更新 | 使用指南、YAML 格式说明 |
| CI 流水线 | GitHub Actions 跑测试 |

---

## 十、关键设计决策

### 10.1 为什么不用 Alembic / SQLAlchemy Migrate？

- tablemaster 的定位是**声明式、YAML 驱动**的轻量工具，面向数据团队而非应用开发者
- Alembic 是命令式的（写 Python migration 脚本），学习成本高
- 我们需要的是 Terraform 式的"desired state → diff → apply"，不是版本化的 migration chain

### 10.2 不处理的范围（V1）

以下功能 V1 **不做**，避免复杂度膨胀：

- 外键（FK）管理 — 跨表依赖复杂，数据团队的分析表通常不用 FK
- 列重命名检测 — 无法区分"删旧列+加新列" vs "重命名"，需要额外元信息
- 数据迁移 — 只做 DDL，不做 DML
- 分区表管理 — 各数据库语法差异大
- 视图 / 存储过程 — 超出表结构管理范畴

### 10.3 破坏性操作的安全策略

- **删列 / 删表** 永远只发出 `⚠ WARNING`，不生成 DDL
- 如果用户确实要删，需要手动写 SQL 或者后续提供 `--allow-destructive` flag
- `ALTER COLUMN TYPE` 在有数据时可能失败（如 `VARCHAR → INT`），apply 时捕获错误即可

### 10.4 类型比较的宽容策略

数据库返回的类型可能带额外信息（如 `int(11)`, `character varying(255)`），需要 normalize：

```python
# MySQL: "int(11)" → "INT", "varchar(255)" → "VARCHAR(255)"
# PG: "character varying(255)" → "VARCHAR(255)", "integer" → "INT"
```

比较时只看 normalize 后的结果，减少误报。

---

## 十一、依赖变更

```toml
# pyproject.toml 新增
[project.optional-dependencies]
schema = ["SQLAlchemy>=2.0", "PyMySQL>=1.1", "psycopg2-binary>=2.9"]
all = [
    "tablemaster[mysql,feishu,gspread,local,schema]",
]
```

`pyyaml` 和 `typer` 已在核心依赖中，无需额外添加。

---

## 十二、测试策略

| 层级 | 范围 | 工具 |
|---|---|---|
| 单元测试 | YAML 解析、类型映射、diff 逻辑、DDL 生成 | pytest + mock |
| 集成测试 | 端到端 init → plan → apply | Docker Compose（MySQL 8 + PG 16） |
| 回归测试 | 现有 database.py 功能不受影响 | 现有测试 + 新增覆盖 |

重点测试场景：
- 空数据库 → plan 应全部是 CREATE
- 结构完全一致 → plan 应为空（"No changes"）
- 列类型不同 → plan 正确识别
- YAML 中删除了一列 → plan 出现 WARNING 而非 DROP
- 同一连接下多个分层目录的表全部被扫描到

---

## 十三、后续展望（V2+）

- **`tablemaster schema diff <connection> --from-commit <hash>`**：结合 Git，查看两个版本间的 schema 变更
- **Migration 记录表**：在目标库中建 `_tm_migrations` 表，记录每次 apply 的时间和内容
- **`--allow-destructive`** flag：允许生成 DROP COLUMN / DROP TABLE 的 DDL
- **外键管理**：分析依赖图，按拓扑序执行 DDL
- **多数据库一键 plan/apply**：`tablemaster plan --all`
- **Web Dashboard**：可视化查看所有连接的 schema 状态
