# Testing Shelfard

There are four ways to test Shelfard, ranging from fast unit tests to a full interactive sandbox.

| Method | What it covers | Speed | Network |
|---|---|---|---|
| [Unit tests](#1-unit-tests) | Core logic: diffing, type mapping, registry, parsers | ~1 s | None |
| [REST integration tests](#2-rest-integration-tests) | REST reader end-to-end with a real mock HTTP server | ~1 s | None (mock) |
| [Docker smoke tests](#3-docker-smoke-test-image) | Full CLI install + all commands against a live API | ~30 s | JSONPlaceholder |
| [Docker playground](#4-docker-playground-image) | Interactive exploration in an isolated environment | — | JSONPlaceholder (build only) |

---

## 1. Unit tests

**File:** `run_tests.py`
**Count:** 47 tests
**Framework:** custom minimal runner (no pytest required)
**Network:** none

Covers the full pipeline without any external dependencies:

- `ColumnSchema` / `TableSchema` construction and serialization
- `ColumnType` normalization for SQLite and REST types
- `schema_comparison` — all severity classifications (SAFE / WARNING / BREAKING), STRUCT recursion, dot-notation naming
- `type_normalizer` — widening rules, `extract_length`
- `LocalFileRegistry` — register, retrieve, version counting, consumer subscriptions (full and projected), impact analysis
- `json_file_reader` / `json_reader` — infer schema from JSON files and dicts

### Run

```bash
# Using the conda env
conda run -n shelfard python3 run_tests.py

# Or with the active env
python3 run_tests.py
```

### Expected output

```
Running 47 tests...

test_column_schema_creation ................ PASS
test_table_schema_creation ................. PASS
...
test_consumer_impact_analysis .............. PASS

47/47 tests passed.
```

### Registry isolation

Tests that write to the registry patch the default instance's root directory:

```python
import tempfile
from pathlib import Path
from shelfard import registry

with tempfile.TemporaryDirectory() as tmp:
    registry._default._root = Path(tmp)
    # ... test code ...
```

This redirects all registry calls (including those from parsers that import module-level shims) to a temporary directory that is cleaned up automatically.

---

## 2. REST integration tests

**File:** `tests/test_rest_reader.py`
**Count:** 7 tests
**Network:** none — uses a real `http.server` on a random port bound to `127.0.0.1`

Covers `RestEndpointReader` end-to-end:

- Happy path: JSON object → typed `TableSchema`
- Nested objects → `STRUCT` columns (recursive)
- Arrays → `ARRAY` column type
- Auth headers forwarded correctly (Bearer token, custom headers)
- HTTP error responses → `ToolResult(success=False)`
- Non-JSON response bodies → `ToolResult(success=False)`

### Run

```bash
conda run -n shelfard python3 tests/test_rest_reader.py
```

### Expected output

```
Running 7 tests...

test_basic_schema .......................... PASS
test_nested_struct ......................... PASS
test_array_field ........................... PASS
test_bearer_auth ........................... PASS
test_custom_headers ........................ PASS
test_http_error ............................ PASS
test_non_json_response ..................... PASS

7/7 tests passed.
```

---

## 3. Docker smoke test image

**Image:** `shelfard-test`
**Script:** `docker/test.sh`
**Network:** JSONPlaceholder (`jsonplaceholder.typicode.com`)

Each `docker run` does a completely fresh `pip install` of shelfard from the local source, then exercises every CLI command in sequence. The container exits with code `0` on success or non-zero on any failure, making it suitable for use in CI or pre-release checks.

### What it tests

| Step | Command |
|---|---|
| Install | `pip install -e /shelfard` |
| Snapshot | `shelfard rest snapshot .../todos/1 --name todos` |
| Snapshot | `shelfard rest snapshot .../users/1 --name users` |
| No-drift check | `shelfard rest check .../todos/1 --name todos` |
| No-drift check | `shelfard rest check .../users/1 --name users` |
| Show schema | `shelfard show todos` |
| Show schema | `shelfard show users` |
| List | `shelfard list schemas` |
| Subscribe (full) | `shelfard subscribe todos --consumer analytics` |
| Subscribe (projection) | `shelfard subscribe users --consumer reporting --columns email,username,phone` |
| List subscriptions | `shelfard list subscriptions` |

### Build and run

```bash
# Build (only needed once, or after source changes)
docker build -f docker/Dockerfile.test -t shelfard-test .

# Run — fresh install + all CLI checks every time
docker run --rm shelfard-test
```

### Expected output

```
┌─────────────────────────────────────────────────┐
│   Shelfard CLI smoke tests                      │
└─────────────────────────────────────────────────┘

── Installing shelfard
   ✓ Installed

── rest snapshot  →  todos
Fetching https://jsonplaceholder.typicode.com/todos/1 …
✓ Snapshot saved: 'todos' (version 1, 4 top-level columns)
   ✓ Snapshot saved

...

╔═════════════════════════════════════════════════╗
║   All smoke tests passed!                       ║
╚═════════════════════════════════════════════════╝
```

### Rebuild after source changes

The source is baked into the image at build time, so rebuild whenever you change the shelfard package:

```bash
docker build -f docker/Dockerfile.test -t shelfard-test . && docker run --rm shelfard-test
```

---

## 4. Docker playground image

**Image:** `shelfard-playground`
**Network:** JSONPlaceholder (during `docker build` only)

An interactive sandbox with shelfard pre-installed and the registry pre-seeded with real data. Use it to manually explore commands, test the agent, or verify behaviour in an isolated environment without touching your local registry.

### Pre-seeded registry

| Name | Columns | Source |
|---|---|---|
| `todos` | 4 (userId, id, title, completed) | JSONPlaceholder `/todos/1` |
| `users` | 8 (id, name, username, email, address STRUCT, phone, website, company STRUCT) | JSONPlaceholder `/users/1` |
| `posts` | 4 (userId, id, title, body) | JSONPlaceholder `/posts/1` |

Pre-registered consumers:

| Consumer | Schema | Columns |
|---|---|---|
| `analytics` | `todos` | all columns |
| `reporting` | `users` | email, username, phone |

### Build and run

```bash
docker build -f docker/Dockerfile.playground -t shelfard-playground .
docker run --rm -it shelfard-playground
```

To persist registry changes across sessions, mount a named volume:

```bash
docker run --rm -it -v shelfard-data:/shelfard/schemas shelfard-playground
```

### Welcome screen

```
  ┌──────────────────────────────────────────────────────┐
  │  Shelfard playground — schema drift detection        │
  │                                                      │
  │  Pre-loaded schemas:   todos  users  posts           │
  │  Pre-loaded consumers: analytics (todos)             │
  │                        reporting (users)             │
  │                                                      │
  │  Try:                                                │
  │    shelfard list schemas                             │
  │    shelfard show users                               │
  │    shelfard list subscriptions                       │
  │    shelfard rest check <url> --name <name>           │
  └──────────────────────────────────────────────────────┘
```

### Things to try

```bash
# Inspect the pre-seeded registry
shelfard list schemas
shelfard show users          # shows nested STRUCT: address, geo, company
shelfard list subscriptions

# Re-check an endpoint to verify no drift since the build
shelfard rest check https://jsonplaceholder.typicode.com/users/1 --name users

# Snapshot a new endpoint and subscribe a consumer
shelfard rest snapshot https://jsonplaceholder.typicode.com/comments/1 --name comments
shelfard subscribe comments --consumer analytics --columns email,body

# Start the interactive agent (requires ANTHROPIC_API_KEY or OPENAI_API_KEY)
export ANTHROPIC_API_KEY=sk-...
shelfard agent
```

---

## CI

Every push and pull request runs the full test suite on GitHub Actions:

```
.github/workflows/python-package-conda.yml
```

Steps:
1. Checkout
2. Set up Python 3.12
3. `pip install -e .` — installs shelfard and all dependencies
4. `flake8` — checks for syntax errors and undefined names (`E9`, `F63`, `F7`, `F82`)
5. `python run_tests.py` — 47 unit tests
6. `python tests/test_rest_reader.py` — 7 REST integration tests

Exit code `0` = all green. The workflow does not run the Docker smoke tests (those require Docker and network access); run them locally before releases.
