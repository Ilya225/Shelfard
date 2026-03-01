# Shelfard — Schema Drift Detection

## Project Purpose

Shelfard is a schema drift detection tool for REST APIs and databases. It captures versioned snapshots of data source shapes, classifies every change as SAFE, WARNING, or BREAKING, tracks which downstream consumers are impacted, and stores check configurations for repeatable on-demand or CI-driven runs.

The system is built in layers: **Acquisition** (vendor readers normalize raw schemas), **Registry** (versioned, pluggable storage), **Checkers** (stored run configs with env-var-resolved auth), and **MCP/Agent** (a standalone MCP server and a conversational assistant that expose all tools to LLM clients). Deterministic drift classification is kept strictly separate from LLM reasoning — the agent handles interpretation and orchestration, not severity decisions.

---

## Architecture

Layered design — each layer is deterministic and agent-ready:

- **Layer 1 — Acquisition** (`shelfard/readers/`): Vendor-specific readers extract raw schemas and normalize them to `TableSchema`; document parsers live in `shelfard/parsers/`
- **Layer 1 — Registry** (`shelfard/registry/`): Pluggable registry with a `SchemaRegistry` ABC and a `LocalFileRegistry` implementation. Tracks both source schema versions and consumer subscriptions (full or projected). Stubs exist for S3, GCS, and SQL backends.
- **Layer 2 — Comparison** (`shelfard/schema_comparison.py`): Pure deterministic diffing, produces self-documenting `SchemaDiff`
- **Layer 2 — Checkers** (`shelfard/checkers/`): Stored run configurations for drift checks. A `RestCheckerConfig` is registered once (URL, headers with `$VAR` placeholders, required env var names) and can then be run on demand. `RestChecker.run()` resolves env vars at call time, never at storage time.
- **Layer 3 — MCP Server** (`shelfard/mcp_server.py`): Standalone FastMCP server exposing registry and checker tools over stdio — `get_schema` (includes checker info if registered), `get_schemas`, `get_subscriptions`, `get_subscription`, `register_checker`, `get_checker_config`, `live_check_schema`. Any MCP client (Claude Desktop, Cursor, etc.) can connect directly.
- **Layer 3 — Agent** (`shelfard/agent.py`): Interactive LangChain 1.x assistant; spawns the MCP server as a subprocess via `MultiServerMCPClient` and gets its tools from there. Supports Claude and OpenAI; model resolved via `--model` flag or env-var auto-detection.
- **Future layers** (planned): Autonomous remediation suggestions, background drift monitoring, consumer-aware alerting

All tools return `ToolResult` with: `success`, `data`, `error`, `next_action_hint`.

---

## CLI Commands

| Command | Description |
|---|---|
| `shelfard rest snapshot <url> --name NAME` | Fetch a REST endpoint and save its schema as a baseline |
| `shelfard rest check <url> --name NAME` | Fetch and diff against the saved baseline; exit `1` on drift |
| `shelfard show <table>` | Display a registered schema; also shows checker type if one is registered |
| `shelfard list schemas` | List all registered source schemas (name, columns, versions, source, latest version) |
| `shelfard list subscriptions` | List all consumer subscriptions across all tables |
| `shelfard subscribe <table> --consumer NAME [--columns COL1,COL2,...]` | Subscribe a consumer to a schema (full or projected) |
| `shelfard checker register <name> --url URL [--header KEY=VALUE ...] [--env VAR ...]` | Register a stored drift-check config for a schema |
| `shelfard checker run <name>` | Run the registered checker against the live endpoint; exit `1` on drift |
| `shelfard checker show <name>` | Display the stored checker config (url, env vars, headers) |
| `shelfard checker list` | List all registered checkers |
| `shelfard agent [--model MODEL]` | Interactive schema assistant; spawns MCP server internally; auto-detects Claude or OpenAI from env |
| `shelfard mcp` | Start the MCP server (stdio transport) — for use with Claude Desktop, Cursor, or any MCP client |

Exit codes: `0` = success / no drift, `1` = drift detected, `2` = error.

---

## File Map

```
Shelfard/
├── shelfard/                        # Python package — all source lives here
│   ├── __init__.py               # Re-exports all public symbols
│   ├── models.py                 # Core data structures: ColumnSchema, TableSchema, ConsumerSubscription, SchemaDiff, RestCheckerConfig, etc.
│   ├── cli.py                    # CLI entry point — show, list, subscribe, rest snapshot/check, checker, agent
│   ├── registry/                 # Pluggable registry package
│   │   ├── __init__.py           # Re-exports + _default LocalFileRegistry instance (backward-compat shims)
│   │   ├── base.py               # SchemaRegistry ABC — 12 methods (source schemas, subscriptions, impact analysis, checkers)
│   │   ├── local.py              # LocalFileRegistry(registry_dir=None) — file-based implementation
│   │   ├── s3.py                 # S3Registry(bucket, prefix) — stub
│   │   ├── gcs.py                # GCSRegistry(bucket, prefix) — stub
│   │   └── sql.py                # SQLRegistry(connection_string) — stub
│   ├── checkers/                 # Stored drift-check configurations
│   │   ├── __init__.py           # Re-exports Checker, RestChecker
│   │   ├── base.py               # Checker ABC — run() → ToolResult
│   │   └── rest.py               # RestChecker — env var resolution, fetch via RestEndpointReader, diff
│   ├── mcp_server.py             # FastMCP server — get_schema (+ checker info), get_schemas, get_subscriptions, get_subscription, register_checker, get_checker_config, live_check_schema (stdio)
│   ├── agent.py                  # run_agent() async REPL — spawns MCP server via MultiServerMCPClient, supports Claude + OpenAI
│   ├── schema_comparison.py      # Layer 2: Diff schemas, classify changes by severity
│   ├── type_normalizer.py        # Vendor-agnostic utilities: TYPE_WIDENING_RULES, is_safe_widening, extract_length
│   ├── readers/                  # Live source readers — each vendor is its own package
│   │   ├── __init__.py           # Re-exports reader classes and functions
│   │   ├── base.py               # SchemaReader ABC (get_schema(), list_tables()) — target set in constructor
│   │   ├── sqlite/               # _TYPE_MAP + SQLiteReader(db_path, table_name) + get_sqlite_schema, list_sqlite_tables
│   │   ├── rest/                 # RestEndpointReader(url, schema_name, *, bearer_token=, headers=) + get_rest_schema
│   │   ├── postgres/             # _TYPE_MAP + _normalize_type (reader implementation pending)
│   │   ├── snowflake/            # _TYPE_MAP + _normalize_type (reader implementation pending)
│   │   └── bigquery/             # _TYPE_MAP + _normalize_type (reader implementation pending)
│   └── parsers/                  # Document parsers — not live sources, no SchemaReader ABC
│       ├── __init__.py           # Re-exports all parser functions
│       ├── json_reader.py        # get_schema_from_json (dict → TableSchema deserializer)
│       └── json_file_reader.py   # infer_schema_from_json_file, read_and_register_json_file
├── tests/
│   └── test_rest_reader.py       # 7 REST integration tests (mock HTTP server, no real network)
├── docker/
│   ├── Dockerfile.test           # Smoke test image — fresh pip install + all CLI commands on every `docker run`
│   ├── Dockerfile.playground     # Interactive sandbox — shelfard pre-installed, registry pre-seeded
│   └── test.sh                   # Entrypoint script for Dockerfile.test
├── docs/
│   └── test.md                   # Testing guide: unit tests, integration tests, CI, Docker images
├── .github/workflows/
│   └── python-package-conda.yml  # CI: pip install + flake8 + run_tests.py + test_rest_reader.py on every push/PR
├── .dockerignore                 # Excludes schemas/, egg-info, caches from Docker build context
├── pyproject.toml                # Packaging metadata and entry point (shelfard = "shelfard.cli:main")
├── Formula/shelfard.rb           # Homebrew formula (copy to homebrew-shelfard tap repo to publish)
├── run_tests.py                  # 47 unit tests covering the full pipeline (no external test framework)
├── schemas/                      # File-based registry root (auto-created on first write)
│   ├── sources/                  # Versioned source schema files — one JSON per table
│   ├── consumers/                # Consumer subscriptions — one JSON per consumer/table pair
│   └── checkers/                 # Checker configs — one JSON per schema (not versioned, always overwrites)
└── CLAUDE.md
```

Importing: `from shelfard import ColumnSchema, get_sqlite_schema, compare_schemas, get_all_schemas, LocalFileRegistry, subscribe_consumer, ...`

### Adding a new registry backend
1. Create `shelfard/registry/<backend>.py` with a class extending `SchemaRegistry`
2. Implement all 12 abstract methods (source schemas, consumer subscriptions, impact analysis, checkers)
3. Re-export from `shelfard/registry/__init__.py` and `shelfard/__init__.py`

### Adding a new vendor reader
1. Create `shelfard/readers/<vendor>/` package with `_TYPE_MAP`, `_normalize_type()`, and a class implementing `SchemaReader`
2. `get_schema(self)` takes **no arguments** — the target (table name, endpoint URL, etc.) is stored in `__init__`
3. Add module-level wrapper functions (`get_<vendor>_schema`, `list_<vendor>_tables`) in the package `__init__.py`
4. Re-export from `shelfard/readers/__init__.py` and `shelfard/__init__.py`

### Adding a new parser
1. Create `shelfard/parsers/<format>_reader.py` (does NOT implement `SchemaReader` — parsers are document-based, not live sources)
2. Re-export from `shelfard/parsers/__init__.py` and `shelfard/__init__.py`

### type_normalizer.py responsibilities
Contains only vendor-agnostic logic — nothing in this file knows about raw SQL type strings:
- `TYPE_WIDENING_RULES` — which `ColumnType` → `ColumnType` transitions are safe
- `is_safe_widening(from_type, to_type)` — used by `schema_comparison.py`
- `extract_length(raw_type)` — parses `varchar(255)` → `255`, used by `readers/sqlite/`

Each vendor's raw-type-to-`ColumnType` mapping lives exclusively in its own reader file.

---

## Tech Stack

- **Language**: Python 3.12 (conda env: `shelfard`)
- **Dependencies**: `requests` (REST reader), `langchain` + `langchain-anthropic` + `langchain-openai` (agent), `mcp` + `langchain-mcp-adapters` (MCP server + client); all other code is stdlib. Declared in `pyproject.toml`.
- **Supported sources**: SQLite, REST API endpoints; PostgreSQL, Snowflake, BigQuery (type maps only, readers pending)

### Running tests
```bash
# Unit tests (47)
conda run -n shelfard python3 run_tests.py

# REST integration tests (7) — uses mock HTTP server, no real network needed
conda run -n shelfard python3 tests/test_rest_reader.py
```

### Docker — smoke test (rerunnable, fresh install each time)
```bash
docker build -f docker/Dockerfile.test -t shelfard-test .
docker run --rm shelfard-test
```

### Docker — interactive playground (pre-seeded registry)
```bash
docker build -f docker/Dockerfile.playground -t shelfard-playground .
docker run --rm -it shelfard-playground
```

See `docs/test.md` for the full testing guide.

---

## Core Data Models

- **`ColumnType`** (enum): 13 canonical types — `INTEGER`, `BIGINT`, `FLOAT`, `DECIMAL`, `VARCHAR`, `TEXT`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `JSON`, `ARRAY`, `STRUCT`, `UNKNOWN`
- **`ColumnSchema`**: Column metadata — type, nullability, length, precision, default, description, and optionally `fields: list[ColumnSchema]` for `STRUCT` columns (recursive)
- **`TableSchema`**: Full table — columns, partition keys, clustering keys, source tracking. The root-level schema is conceptually the top-level STRUCT.
- **`ConsumerSubscription`**: A named consumer's dependency on a source schema. `subscribed_columns=None` means a full snapshot; a list means a projection. Stores the `TableSchema` snapshot at subscription time plus the source schema version it was derived from.
- **`RestCheckerConfig`**: Stored configuration for a REST drift check — `schema_name`, `url`, `headers` (list of dicts, values may contain `$VAR` placeholders), `env` (list of required env var names, never values). Serialized as a single (non-versioned) JSON file at `schemas/checkers/<schema_name>.json`.
- **`SchemaDiff`**: Comparison result — list of changes, severity per change, human-readable summaries. Nested STRUCT field changes use dot-notation column names (e.g. `"address.zip"`).

### STRUCT type
`ColumnType.STRUCT` represents a nested object with its own typed sub-fields. It is the recursive building block:
- `ColumnSchema.fields` holds the sub-schema (itself a `list[ColumnSchema]`, each of which can also be STRUCT)
- `json_file_reader` and `RestEndpointReader` infer STRUCT automatically for any nested dict in a JSON payload
- `schema_comparison` recurses into STRUCT fields; changes are reported with qualified names
- `ColumnSchema.from_dict(col)` reconstructs nested schemas from serialized dicts (used by registry, parsers, and comparison)
- `ColumnType.JSON` is kept for opaque/untyped JSON blobs (e.g. vendor columns declared as JSON without a known structure)

---

## Change Severity Rules

| Severity | Examples |
|---|---|
| **SAFE** | Nullable column added, nullability relaxed, type widened (int→bigint, varchar(50)→varchar(200)) |
| **WARNING** | Column reordered, default value changed, DECIMAL precision decreased |
| **BREAKING** | Column removed, NOT NULL column added without default, type narrowed, dangerous type conversion |

---

## Key Conventions

- External dependencies must be justified and declared in `pyproject.toml` `dependencies`; prefer stdlib otherwise
- All public functions return `ToolResult` for LLM agent compatibility
- Severity classification is deterministic — do not involve LLM for clear-cut cases
- `SchemaRegistry` is an ABC — swap backends by instantiating a different class; module-level functions delegate to a `LocalFileRegistry` default instance
- `LocalFileRegistry` storage: `schemas/sources/<table>.json` (versioned list), `schemas/consumers/<consumer>/<table>.json` (versioned list), `schemas/checkers/<schema>.json` (single dict, always overwritten)
- Checker env vars: resolved from `os.environ` at `RestChecker.run()` time; never stored as values. Use `$VAR_NAME` in url or header values; list the name in `env`.
- `SchemaReader.get_schema()` takes no arguments — the target is fixed in the constructor
- Unit tests live in `run_tests.py`; integration tests (requiring network mocks or external resources) live in `tests/`; both use a custom minimal test runner (no pytest)
- Registry test isolation: patch `registry._default._root = Path(tmp)` inside a `tempfile.TemporaryDirectory()` block
- CLI uses argparse with `dest="command"` at the top level; all commands dispatch via `args.func(args)`; `_print_schema(schema_dict, indent)` recurses into STRUCT fields
