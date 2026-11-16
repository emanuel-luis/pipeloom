# ADR-0050: Python-first model authoring

Architecture: [Section 4, Configuration](../architecture.md#4-configuration), with changes in sections 1, 2, 3, 5, 7, 11, 12, 13 and 15

## Context

Since 0.1 a model is declared in YAML ([ADR-0003](0003-model-authoring.md)): a model file lists the model, its `inputs`, its config and either a SQL body or a `python: module:function` pointer, and folder `_defaults.yml` files and `defaults.models` cascade settings onto it. A Python model is therefore written twice, once as an entry in YAML and once as a function whose parameters must match that entry (PLK602), and the config sits in a different file from the code it configures.

The maintainer reversed that choice for 0.9, before 1.0 freezes the config:

- A model is defined in code, with a decorator on a function, as in the author's earlier prototype. YAML keeps what it is good at, as in dbt: the project file holds project and folder defaults, vars, sources and targets, and properties files describe and configure models. Precedence follows dbt: the decorator wins over properties YAML, which wins over folder and project defaults. YAML alone no longer creates a model.
- Every model is a Python function. There are no `sql:` models and no `.sql` files. SQL runs through `ctx.sql(query)`, which runs the query on the target engine against the model's inputs and returns an engine frame.
- The pivot is the 0.9 headline and is designed first, here. Contracts and the parked test and build work are then adapted to the decorator, followed by OpenLineage, sample data and the Airflow WATCHER mode; 0.9.0 ships with all of it.
- The decorator, its model spec and the module loader may be adapted from the author's earlier prototype, renamed to pipeloom's modules and conventions.
- Discovery is hybrid. A benchmark on Windows with polars models (cold: 50, 200 and 1,000 models) measured 1.9, 3.3 and 11.3 s to import every module against 0.07, 0.22 and 1.4 s to parse them with `ast`; with a per-file cache and one changed file, parsing took 0.05 to 0.22 s and importing 1.2 to 1.5 s. Reading the decorator from the AST keeps compiles fast and runs no user code, but forbids dynamic config; importing allows it. The chosen design parses every file and imports only the modules whose decorator arguments are not static. Dependencies stay declared, never traced, and Airflow keeps the manifest load mode by default.

## Options

- **Keep YAML-first ([ADR-0003](0003-model-authoring.md)).** Validates without running code, but every Python model is declared twice and SQL models need a second body kind on every engine.
- **Python-first, static discovery only.** The compiler reads the decorator with `ast` and never imports. Fast and safe, but every argument must be a literal: no config computed from a loop, a helper or a constant module.
- **Python-first, import every model module.** Any Python value works and the code is simple, but a cold compile of 1,000 models takes over 11 s and every compile runs user code, including in the Airflow DAG processor.
- **Python-first, hybrid discovery.** Static where the decorator is static, imported where it is not, with a per-file cache. Fast for the common case, dynamic where needed, at the cost of two code paths that must agree on locations and results.
- **Decorator arguments.** Typed objects only (verbose in simple cases), plain dicts only (no completion or type checking), or both.
- **SQL.** Keep SQL model files beside Python models, or run SQL only through `ctx.sql` inside a Python model.
- **Migration.** A user-facing `pipeloom migrate` command and a deprecation release, or a hand-written guide and removal before 0.9.0.

## Decision

Python-first authoring with hybrid discovery. Every numbered point below is part of the decision.

1. **`@model`.** Keyword-only. Bare `@model` and `@model(...)` both work (two overloads). It returns the same function object with a frozen `__pipeloom_model__ = ModelSpec(kwargs)` that holds only the arguments given, so the function stays callable in a user's own tests; `None` means not set. Nested config accepts plain dicts with the YAML keys or the pydantic config models re-exported under short names: `Write`, `Incremental`, `Column`, `Expectation`, `Quality`, `Deletes` (pydantic's `dataclass_transform` gives typed constructors under `mypy --strict`). Helpers: `ref(name)`, `source(group, table)`, `var(name, default=...)`, and the check helpers `not_null(*cols)`, `unique(*cols)`, `accepted_values(col, values)`, `between(col, min=, max=)` (the `range` check), `regex(col, pattern)`, `row_count(min=, max=)` and `freshness(col, max_age)`, which return a `CheckConfig`. `var(name, default=...)` returns a `Var` marker whose value is resolved at resolve time from the project `vars` and `--vars`. A `Var` is accepted as a top-level `@model` keyword value and anywhere inside a plain list or dict argument (for example `write={"unique_key": var("keys")}`); the typed constructors and check helpers reject it when called, with a hint to use the plain mapping form, so they stay strictly typed. An explicit `None` argument means not set and is dropped from the spec; `var()` without a default is distinct from `default=None`. Plain values and mappings are validated at resolve time, with located diagnostics. Typed constructors and helpers validate when called: in a static module the AST resolver evaluates them itself and maps a pydantic error to PLS102 (or the matching PLS code) at the call's line and column; in a dynamic module the error is raised during import and is PLY005 at the innermost frame in that file. Applying `@model` twice raises.

2. **Discovery (hybrid).**
   - Where models come from. A `model_paths` entry is a directory (every `*.py` below it, skipping dot entries, `__pycache__` and virtualenvs) or a glob containing `*`. The new `model_packages: [dotted.name]` locates each top-level package with `importlib.util.find_spec` and walks it as files. Python models and properties YAML come from the same scan.
   - AST resolver. Each file is parsed. It records the top-level imports `from pipeloom import model [as m]`, `import pipeloom [as pl]` (used as `pl.model`) and `from pipeloom.authoring import ...`, and recognises the helpers the same way. A static expression is a literal accepted by `ast.literal_eval`; a list, tuple, set or dict of static expressions; or a call to a known pipeloom helper or re-exported config class (`Write`, `Incremental`, `Column`, `Expectation`, `Quality`, `Deletes`, also in their `pipeloom.` and alias-qualified forms) whose positional and keyword arguments are all static. A decorator on a top-level `def` is static when every argument is static; pipeloom's helpers and classes are then called on the literal values, and no user code runs. Anything else makes the module dynamic. A module is dynamic, and is imported, when any `@model` argument is not static, when `model` is used other than as a static decorator on a top-level `def` (factories, loops, `model(...)(fn)`), on `from pipeloom import *`, or when it rebinds a helper name. A file that never imports pipeloom is skipped. Keyword names are always checked from the AST, also in dynamic modules: an unknown keyword is PLS101 with a suggestion, and that module is then not imported.
   - Import fallback. `pipeloom.discovery.loader.ModuleLoader` is a context manager grown from today's `CallableLoader` and adapted from the author's earlier prototype loader. The module name is the file path relative to the longest `python_paths` root that contains it. It takes a process-wide lock, puts the roots first on `sys.path`, imports only the dynamic modules and collects the marked functions from `vars(module)` with their bound global name. A function object seen in several modules is kept where `__name__ == fn.__module__`, else first in path order. On exit it removes the roots and purges, by path, every module whose `__file__` or `__path__` lies under them (namespace packages too), except `pipeloom`, `__main__` and `__mp_main__`, so the case harness can load about a hundred projects in one process without one seeing another's modules.
   - Load summary. Loading never stops at the first failure. Each module that cannot be loaded is one PLY005: a syntax error, an exception during import, a file outside every `python_paths` root, or a module name that resolves to a different file. The location is the innermost traceback frame inside that file, or `SyntaxError.lineno` and `offset`.
   - Cache. `<project>/.pipeloom/cache/discovery.json` (already git-ignored), written to a temporary file and then replaced. The key per file is its POSIX path, `mtime_ns`, size, the pipeloom version, the Python minor version and the cache schema. For a static file it stores the discovered models as JSON (arguments, parameters, locations), with a `var()` argument stored as its `Var` marker, never a value, so changing vars never invalidates the cache; for a dynamic file the `dynamic` flag and its decorator sites (parse data only), because import results are never cached; for a file without models `none`. Deleted files are dropped. A schema or version mismatch, or a corrupt cache, means a cold run; a failed write (a read-only file system on Airflow or Databricks) is ignored. The cache is never part of the manifest or the plan hash.
   - Reload. `Discovery.refresh()` stats the files again, re-parses changed static files, and purges and re-imports every dynamic module (a changed helper module purges them all). `docs serve` calls it on each index request and regenerates the site when the snapshot changed.
   - Seam. Discovery returns `DiscoveredModel(name, module, attr, file, line, col, kwargs, kwarg_locations, params, dynamic)` records plus a list of failures. A file may hold several models. The default name is the function name; two models with one name are PLR202 at the second decorator.

3. **Parameters become inputs.** Each function parameter is bound in this order: (1) an `inputs=` entry, whose value is `ref(...)`, `source(...)` or a bare name (a string, resolved by steps 3 and 4); (2) a parameter with a default is never an input unless `inputs=` lists it; (3) a model with that name, never the model itself; (4) a source table with that name, when exactly one source group has it; (5) otherwise PLR201 with fuzzy suggestions over models and sources. A table name found in two source groups is PLR210. `ctx` is reserved and optional: it is passed only when the signature has it, and a model named `ctx` is PLS102. `*args`, positional-only parameters, and an `inputs=` key that is not a parameter (unless the function takes `**kwargs`) are PLK602. A model never reads its own name, so a bronze `orders` model with an `orders` parameter binds the source. `incremental.input` names an input alias exactly as today, so the incremental filter applies to a source input and to a model input alike.

4. **Sources in `pipeloom.yml`.** `sources` maps a group name to a group with `format`, `location`, `schema`, `options`, `tags`, `meta` and `tables`, and each table may set `location` or `table`, `format`, `options` and the other group keys. A table `location` is joined to the group `location`; a table `table` is qualified by the group `schema`. A table entry whose merged result has no `location` and no `table` uses its key as `table`, qualified by the group `schema`; one with both is PLS108. `targets.<t>.sources.<group>` deep-merges over the group, and an explicit `null` removes a key. The unique id is `source.<project>.<group>.<table>`. Sources in properties files are rejected for now.

5. **The `models:` tree in `pipeloom.yml`.** It mirrors the model folders, with config keys prefixed by `+` as in dbt. Root-level `+keys` are project defaults and folder keys nest; for a model found through `model_packages` the folder path is the dotted module path. `vars`, `targets`, `default_target` and the `~/.pipeloom/profiles.yml` overrides stay, with no separate profiles file. `_defaults.yml`, `defaults.models` and model files with `sources:` or `models:` lists are removed (see point 19). `python_paths` stays, with the default `["."]`, and every model file must lie under one of its roots (PLY005 otherwise).

6. **Properties YAML.** Any `*.yml` or `*.yaml` file under `model_paths` except `pipeloom.yml`, with any file name. Its shape is a `models` list whose entries have `name`, `description`, `config` (model settings), `columns` (the contract), `data_tests` and `unit_tests`. An entry never sets `inputs`. An entry that names no model, or a model that another entry already describes, is PLR209 (error), suppressed while any PLY005 exists. `schema export` writes `project.json` and a new `properties.json`; `models.json` and `defaults.json` stop being published under `latest/` (older version folders keep them). The modeline is `$schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/properties.json`.

7. **Precedence.** Decorator, then properties, then the `models:` tree from the folder nearest the model up to the root, then the project root `+keys`, then built-in defaults. The merge rules do not change: mappings deep-merge, lists replace, `tags` is an ordered union, and an explicit `null` removes a key (in YAML only). `columns` is a model key, so the folder tree cannot set it, and decorator columns replace properties columns whole. `ResolvedModel.origins` maps each top-level key to the layer and `Location` it came from, and `locate_model(m, field)` uses it. Origins are not in the manifest.

8. **`ctx.sql(query, *, dialect=None, inputs=None)`.** The query sees the model's inputs by alias, as read (after the incremental filter); `inputs=` adds or replaces frames by name. It returns the native frame, or a Narwhals frame under `frame_api: narwhals`. The dialect is the `dialect` argument, then the inheritable setting `sql_dialect`, then the engine's own. sqlglot `transpile` runs only when the chosen dialect differs from the engine's, in `pipeloom.runtime.sql`, imported lazily (one more import-linter ignore entry). Translation needed with sqlglot missing is PLC513 (L5). Translation happens only when a dialect is declared and differs from the engine's, so sqlglot stays an optional extra and no core dependency is added; validation tells the user when a model's SQL will be translated. pandas raises `SqlUnsupportedError` at run time, reported as PLX101 with a hint, and L6 warns with PLK606 when a function's source contains `ctx.sql` and the engine has no SQL. `EngineCapabilities.sql_dialect: str | None = None` joins plugin API (0, 5), which is unreleased, so the version does not change. polars has no sqlglot dialect; it maps to the closest one and tests cover the mapping.

9. **Views.** A `view` model is a function that returns `ctx.sql(...)`. The runner calls it in view mode: input parameters are placeholders, any use of them other than passing them to `ctx.sql` raises, and `ctx.sql` returns a `ViewQuery`, a new internal type, for the unchanged `create_view`. Input aliases are bound to physical relations as SQL views are today: the runner passes the query with one `ReadSpec` per alias, and the engine binds each alias to that relation (Spark as a CTE over the table name or path). `inputs=` frames are rejected in view mode (PLX120), and returning anything other than the `ViewQuery` is PLX120. PLP409 narrows to "a view cannot inline an ephemeral model". Ephemeral models are unchanged: they are computed inside the consuming unit.

10. **Manifest.** The schema stays `pipeloom.manifest/1`; the change is breaking before 1.0 and the CHANGELOG says so. A node body is `PythonBody{kind: "python", callable: "module:attr", file}`; `node.file` is the `.py` file and `node.line` the decorator line. `SqlBody` is removed with the legacy path, after which an old manifest with `kind: sql` is PLX107. The checksum formula does not change (resolved config, `callable`, NUL, module file bytes, plus `columns` without `description`), so editing any function in a file marks every model of that file modified, which is accepted as conservative. Plan hashes change once. Dynamic config must be deterministic, and PLX103 guards it. `meta` values must be JSON (PLS102).

11. **Diagnostics.** A finding in Python carries the file, the decorator line and column and, for a keyword argument, that argument's own line and column from the AST when the `@model(...)` call is written directly on the `def`, in a static or a dynamic module. A model created any other way (factories, loops, `model(...)(fn)`) is located at the function's file and `__code__.co_firstlineno`, column 1, with no per-argument location, so a diagnostic on an argument points at the function. The JSON pointer reads `/@model/write/unique_key`. Pydantic errors map through `DecoratorSite.locate(pointer)`, which has the same interface as `LoadedDocument.locate`, so `from_validation_error` is reused. `CodeInfo.status` gains `"retired"` with a `retired` version; the catalog page says "Retired in 0.9: no longer emitted", and a retired code has no case file (`test_catalog` changes accordingly). Codes:

    | Code | Layer, severity | Title | Status |
    |---|---|---|---|
    | PLY005 | L0 E | Model module cannot be loaded | new |
    | PLR209 | L2 E | Properties entry names no model, or describes one twice | new |
    | PLR210 | L2 E | Input name matches a table in several source groups | new |
    | PLC513 | L5 E | SQL translation needs the sqlglot extra | new |
    | PLK606 | L6 W | `ctx.sql` on an engine without SQL | new |
    | PLX120 | Runtime E | View model did not return `ctx.sql(...)`, or passed it `inputs=` | new |
    | PLK602 | L6 E | Parameters do not match inputs | reused, same meaning |
    | PLK603, PLK605 | L6 W | Applied to literal `ctx.sql` queries | reused |
    | PLP409 | L4 E | A view cannot inline an ephemeral model | narrowed |
    | PLR201, PLR202, PLR203, PLS101, PLS102, PLS108, PLG302 | | Also located in Python | reused |
    | PLK601, PLK604, PLC502 | | | retired in 0.9 |

    PLM209, PLX116, PLX119, PLC511, PLC512, PLA008 and PLA009 stay free for other 0.9 items, and PLR208, PLX114, PLX115, PLP415 and PLQ303 for the test and build work.

12. **Layers.** L0: YAML codes and PLY005. L1: pydantic over YAML and decorator arguments. L2: references through parameters, PLR209 and PLR210. L3: unchanged. L4: unchanged, with PLP409 narrowed. L5: PLC502 retired, PLC513 added. L6: static checks on the function source: PLK602 (signature against `inputs=`, emitted while binding), PLK603 and PLK605 on literal `ctx.sql` strings, and PLK606.

13. **Runtime.** Executors and tasks do not change. They read `metadata.python_paths` and the node's `callable` from the manifest, and `ModuleLoader.load("module:attr")` imports that one module. An executor running a unit from a manifest never runs discovery. Any invocation that compiles runs it, including `pipeloom run` without `--manifest` and Airflow PROJECT mode, whose unit tasks each compile, so dynamic modules are imported there too. The function is called by keyword: the input aliases, plus `ctx` when it is a parameter. `ModelContext` keeps its fields and gains `sql()`, backed by a private callable that the runner sets.

14. **Contracts.** `columns` comes from the decorator (`Column(...)`) or from properties. `ColumnConfig.description` is optional and stays out of the node checksum and the plan hash: `columns` is hashed without `description`. Where descriptions appear in the manifest and on the docs site is settled later in 0.9, outside the plan hash. [ADR-0044](0044-model-contracts.md) gets an amendment for the sources of `columns`, their precedence and `description`; PLP414 does not change.

15. **Test and build shape** (for the later ADR-0045). `data_tests` (the parked `DataTestConfig`, whose `check` is a `CheckConfig`) is a model key that the decorator (a keyword added by the ADR-0045 change) and properties can set, and a list that replaces. `unit_tests` live only in properties, with `given` keyed by input alias (the parameter name) and `expect`. Plain pytest calls on the function also work.

16. **`init` and `new model`** ([ADR-0041](0041-pipeloom-init.md) amendment). `pipeloom new model NAME [--sql|--python] [--folder]` writes `models/<folder>/<name>.py`. With `--sql` the body returns `ctx.sql("select 1 as id")`, with `--python` a frame stub; the default is `--python` on pandas and `--sql` otherwise. The usage error for a missing `python_paths` goes away. `init` writes one `.py` per layer, the `models:` tree instead of `_defaults.yml`, `models/silver/_properties.yml`, and a `.vscode/settings.json` that maps `properties.json` to `models/**/*.yml`.

17. **Airflow and Databricks.** `MANIFEST` stays the default Airflow load mode and imports nothing at parse or run time except the one module of the unit being run. `PROJECT` imports only the dynamic modules, inside the DAG processor and again in every unit task, which compiles (point 13), and its cache key adds the `model_packages` directories ([ADR-0037](0037-airflow-packaging-and-rendering.md) amendment). The Databricks upload set is `pipeloom.yml`, the nodes' `.py` files and every `*.py` under `python_paths`, plus the manifest; properties YAML is uploaded too, for parity. Modules from `model_packages` must be installed in the job environment, and the docs say so ([ADR-0035](0035-databricks-rendering-and-job-lifecycle.md) amendment).

18. **Docs.** A migration section in the CHANGELOG under 0.9.0 and a new guide, `docs/guide/migrate-0.9.md`, both written with the removal of the legacy path.

19. **Transition.** The legacy YAML `models:` path and the new path coexist on main, with no warning code, until the change that removes the legacy path; one project may mix both. That change lands before 0.9.0, and 0.9.0 ships Python-first only.

**Migration and scope decisions.**

- There is no user-facing `pipeloom migrate` command. An internal, committed converter, `tools/migrate_yaml_models.py`, migrates the repository's own fixtures, cases and examples and is always called with the same command line. The migration guide shows the mapping from YAML to the decorator by hand. The tool is deleted once nothing in the repository is declared in YAML.
- 0.9.0 drops YAML models with no deprecation release. Removal before 1.0 is allowed by pre-1.0 SemVer and by [ADR-0016](0016-versioning-and-plugin-api.md).
- `ctx.sql` translates only when a dialect is declared (`dialect=` or `sql_dialect`) and differs from the engine's (point 8).
- [ADR-0019](0019-yaml-dialect.md) does not change; its YAML dialect now covers the project file and properties files only.

**Rejected.**

- Keeping YAML-first: the double declaration of Python models is the problem this ADR solves.
- Static-only discovery: forbids any computed config, which the maintainer wants to allow.
- Importing every module: over 11 s for 1,000 models cold, and user code on every compile.
- Typed objects only, or dicts only, for decorator arguments: the first is verbose for simple cases, the second has no completion. Both are accepted.
- SQL model files beside Python models: two body kinds on every engine for no gain over `ctx.sql`.
- A module-level registry of decorated functions: the decorator only marks the function, and discovery collects marked functions into the project being compiled (principle 3).
- sqlglot as a core dependency, or translating every query: translation runs only when a declared dialect differs from the engine's.
- A user-facing `pipeloom migrate` command and a deprecation release (see above).

**Example.** The stub of the decorator:

```python
F = TypeVar("F", bound=Callable[..., Any])

@overload
def model(fn: F, /) -> F: ...
@overload
def model(*, name: str | None = None, description: str | None = None,
          inputs: Mapping[str, str | Ref | SourceRef] | None = None,
          materialized: Literal["table", "incremental", "view", "ephemeral", "intermediate"] | None = None,
          incremental: Incremental | Mapping[str, Any] | None = None,
          write: Write | Mapping[str, Any] | None = None,
          partition_by: Sequence[str] | None = None, cluster_by: Sequence[str] | None = None,
          columns: Sequence[Column | Mapping[str, Any]] | None = None,
          expectations: Sequence[Expectation | Mapping[str, Any]] | None = None,
          quality: Quality | Mapping[str, Any] | None = None,
          tags: Sequence[str] | None = None, retries: int | None = None,
          retry_delay: str | float | None = None, retry_on: Literal["transient", "any"] | None = None,
          timeout: str | float | None = None, frame_api: Literal["native", "narwhals"] | None = None,
          sql_dialect: str | None = None, schema: str | None = None, catalog: str | None = None,
          meta: Mapping[str, Any] | None = None) -> Callable[[F], F]: ...
# ADR-0045 adds: data_tests: Sequence[DataTest | Mapping[str, Any]] | None = None
```

`models/silver/orders_enriched.py`:

```python
import polars as pl
from pipeloom import Column, Expectation, ModelContext, Write, model, not_null, source

@model(
    materialized="incremental",
    inputs={"customer": source("tpch", "customer")},
    incremental={"input": "orders", "column": "o_orderdate", "lookback": "3d"},
    write=Write(strategy="merge", unique_key=["order_key"]),
    columns=[
        Column(name="order_key", dtype="int64", nullable=False),
        Column(name="customer_name", dtype="string"),
        Column(name="order_date", dtype="date"),
        Column(name="total_price", dtype="decimal(15,2)"),
    ],
    expectations=[Expectation(name="keys", check=not_null("order_key"), on_violation="fail")],
)
def orders_enriched(ctx: ModelContext, orders: pl.LazyFrame, customer: pl.LazyFrame) -> pl.LazyFrame:
    return ctx.sql("""
        select o.o_orderkey as order_key, c.c_name as customer_name,
               o.o_orderdate as order_date, o.o_totalprice as total_price
        from orders o join customer c on o.o_custkey = c.c_custkey
    """)
```

`orders` binds to the source table `tpch.orders` by parameter name (point 3, step 4) and `customer` through `inputs=`. `orders` is both a source input and the incremental input, and the incremental filter applies to it as it does today. Every argument is static, so this file is resolved without being imported.

`pipeloom.yml`:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/project.json
config_version: 1
name: tpch_polars
model_paths: [models]
vars:
  tpch_dir: ../data/tpch
models:
  +materialized: table
  silver:
    +catalog: pipeloom_silver
    +tags: [silver]
sources:
  tpch:
    format: parquet
    location: ${var:tpch_dir}
    tables:
      customer:
        location: customer.parquet
      orders:
        location: orders.parquet
targets:
  dev:
    engine:
      kind: polars
    catalog:
      kind: filesystem
      root: target/warehouse
    state:
      kind: json
      path: .pipeloom/state
  prod:
    engine:
      kind: spark
    catalog:
      kind: unity
      catalog: main
    sources:
      tpch:
        format: table
        location: null
        schema: samples.tpch
        tables:
          customer:
            location: null
          orders:
            location: null
```

`models/silver/_properties.yml`:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/properties.json
models:
  - name: orders_enriched
    description: One row per order with its customer name.
    config:
      retries: 2
    data_tests:
      - name: order_key_unique
        check:
          unique: [order_key]
        on_violation: fail
    unit_tests:
      - name: joins_customer
        given:
          orders:
            rows:
              - o_orderkey: 1
                o_custkey: 7
                o_orderdate: 2024-01-02
                o_totalprice: 10.5
          customer:
            rows:
              - c_custkey: 7
                c_name: Ana
        expect:
          rows:
            - order_key: 1
              customer_name: Ana
              order_date: 2024-01-02
              total_price: 10.5
```

## Consequences

- A model is written once, in one file, with completion and type checking from the decorator's signature. SQL users write a one-line function around `ctx.sql`.
- Most projects compile without running user code: only modules with dynamic decorator arguments are imported, and the per-file cache keeps warm compiles fast. Where a module is imported, compiling runs user code, in Airflow PROJECT mode and in `docs generate` in CI among other places, so the non-goal becomes "user code runs at compile time only for modules with dynamic decorator arguments". Airflow's default MANIFEST mode still imports nothing.
- The engine contract shrinks: SQL model bodies are gone, and engines declare a `sql_dialect` instead.
- Every existing project must be rewritten before it runs on 0.9.0, every manifest and plan hash changes once, and `models.json` and `defaults.json` stop being published. The migration guide covers the mapping.
- Two discovery paths (AST and import) must produce the same `DiscoveredModel` records and the same locations; the case harness covers both for every Python-located code.
- Editing any function in a module marks every model of that module modified under `state:modified`.
- ADR-0003 is superseded. ADR-0035, 0037, 0040, 0041, 0042, 0044 and 0047 carry amendments for this change.

Risks:

- polars has no sqlglot dialect, so translation to polars maps to the closest dialect and may differ on edge cases; tests cover the mapping.
- The cache misses an edit that keeps both mtime and size; a cold run fixes it.
- Non-deterministic dynamic config trips PLX103 and can go stale in the Airflow PROJECT compile cache.
- A model folder named like an installed package is shadowed by it; PLY005 must say why.
- Importing a dynamic module at compile time runs user code (Airflow PROJECT mode, `docs generate` in CI). In PROJECT mode every unit task compiles, so dynamic modules are also imported on workers.

## Status

Accepted. Supersedes [ADR-0003](0003-model-authoring.md).

## Date

2026-11-15
