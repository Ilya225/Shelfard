"""
Unit tests for the PostgreSQL reader and checker.

Uses unittest.mock to simulate psycopg2 — no live database needed.

Run: conda run -n shelfard python3 tests/postgresql_tests.py
"""

import os
import sys
import tempfile
import traceback
from collections import namedtuple
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from shelfard import LocalFileRegistry
from shelfard.models import PostgresCheckerConfig

# ─────────────────────────────────────────────
# Minimal test framework
# ─────────────────────────────────────────────

passed = 0
failed = 0
errors = []


def test(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  ✓ {name}")
        passed += 1
    except AssertionError as e:
        print(f"  ✗ {name}")
        errors.append((name, str(e)))
        failed += 1
    except Exception as e:
        print(f"  ✗ {name} [ERROR]")
        errors.append((name, traceback.format_exc()))
        failed += 1


def section(name):
    print(f"\n── {name} ──")


# ─────────────────────────────────────────────
# Mock helpers
# ─────────────────────────────────────────────

# Mirrors psycopg2's Column namedtuple (DB-API 2.0)
_PgCol = namedtuple(
    "Column",
    ["name", "type_code", "display_size", "internal_size", "precision", "scale", "null_ok"],
)


def _make_pg_mock(fetchall_side_effects, description=None):
    """Build a mock psycopg2 module with a pre-configured connection + cursor."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    if fetchall_side_effects is not None:
        mock_cursor.fetchall.side_effect = fetchall_side_effects
    if description is not None:
        mock_cursor.description = description

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_pg = MagicMock()
    mock_pg.connect.return_value = mock_conn
    return mock_pg, mock_cursor


# ─────────────────────────────────────────────
# PostgreSQL Reader
# ─────────────────────────────────────────────

section("PostgreSQL Reader")


def test_postgres_normalize_type():
    from shelfard.tools.postgres.reader import _normalize_type
    from shelfard.models import ColumnType
    assert _normalize_type("integer")              == ColumnType.INTEGER
    assert _normalize_type("INT4")                 == ColumnType.INTEGER
    assert _normalize_type("bigint")               == ColumnType.BIGINT
    assert _normalize_type("double precision")     == ColumnType.FLOAT
    assert _normalize_type("character varying")    == ColumnType.VARCHAR
    assert _normalize_type("character varying(50)")== ColumnType.VARCHAR
    assert _normalize_type("jsonb")                == ColumnType.JSON
    assert _normalize_type("timestamp with time zone") == ColumnType.TIMESTAMP
    assert _normalize_type("timestamptz")          == ColumnType.TIMESTAMP
    assert _normalize_type("bpchar")               == ColumnType.VARCHAR
    assert _normalize_type("uuid")                 == ColumnType.UNKNOWN  # not mapped

test("PostgreSQL _normalize_type maps vendor types correctly", test_postgres_normalize_type)


def test_postgres_reader_table_mode():
    """Table mode: information_schema rows → ColumnSchema list with correct types + nullability."""
    from shelfard.tools.postgres.reader import PostgresReader

    # (col_name, data_type, is_nullable, char_max_len, num_precision, num_scale, col_default)
    info_rows = [
        ("id",   "integer",           "NO",  None, 32,   0,    None),
        ("name", "character varying", "YES", 100,  None, None, None),
        ("score","numeric",           "NO",  None, 10,   2,    None),
    ]
    mock_pg, _ = _make_pg_mock([info_rows])
    with patch("shelfard.tools.postgres.reader.psycopg2", mock_pg):
        result = PostgresReader("postgresql://localhost/test", "users", table="users").get_schema()

    assert result.success, result.error
    schema = result.data["schema"]
    assert schema["table_name"] == "users"
    assert schema["source"] == "postgresql"
    cols = {c["name"]: c for c in schema["columns"]}
    assert cols["id"]["col_type"]      == "integer"
    assert cols["id"]["nullable"]      == False
    assert cols["name"]["col_type"]    == "varchar"
    assert cols["name"]["nullable"]    == True
    assert cols["name"]["max_length"]  == 100
    assert cols["score"]["col_type"]   == "decimal"
    assert cols["score"]["nullable"]   == False
    assert cols["score"]["precision"]  == 10
    assert cols["score"]["scale"]      == 2

test("PostgresReader table mode: information_schema → ColumnSchemas", test_postgres_reader_table_mode)


def test_postgres_reader_query_all_nonnull():
    """Query mode: zero NULLs in sampled rows → columns marked NOT NULL by contract."""
    from shelfard.tools.postgres.reader import PostgresReader

    desc = [
        _PgCol("id",   23, None,  4, None, None, None),  # OID 23 = int4
        _PgCol("email", 25, None, -1, None, None, None),  # OID 25 = text
    ]
    sample_rows = [(1, "alice@example.com"), (2, "bob@example.com")]
    oid_rows    = [(23, "int4"), (25, "text")]

    mock_pg, _ = _make_pg_mock([sample_rows, oid_rows], description=desc)
    with patch("shelfard.tools.postgres.reader.psycopg2", mock_pg):
        result = PostgresReader(
            "postgresql://localhost/test", "users",
            query="SELECT id, email FROM users",
        ).get_schema()

    assert result.success, result.error
    cols = {c["name"]: c for c in result.data["schema"]["columns"]}
    assert cols["id"]["nullable"]    == False  # no NULLs → NOT NULL by contract
    assert cols["email"]["nullable"] == False

test("PostgresReader query mode: no NULLs in sample → NOT NULL by contract", test_postgres_reader_query_all_nonnull)


def test_postgres_reader_query_has_null():
    """Query mode: any NULL in sampled rows → column is nullable."""
    from shelfard.tools.postgres.reader import PostgresReader

    desc = [
        _PgCol("id",   23, None,  4, None, None, None),
        _PgCol("note", 25, None, -1, None, None, None),
    ]
    # note column has a NULL in second row
    sample_rows = [(1, "hello"), (2, None)]
    oid_rows    = [(23, "int4"), (25, "text")]

    mock_pg, _ = _make_pg_mock([sample_rows, oid_rows], description=desc)
    with patch("shelfard.tools.postgres.reader.psycopg2", mock_pg):
        result = PostgresReader(
            "postgresql://localhost/test", "events",
            query="SELECT id, note FROM events",
        ).get_schema()

    assert result.success, result.error
    cols = {c["name"]: c for c in result.data["schema"]["columns"]}
    assert cols["id"]["nullable"]   == False  # no NULLs
    assert cols["note"]["nullable"] == True   # has a NULL

test("PostgresReader query mode: NULL in sample → column is nullable", test_postgres_reader_query_has_null)


def test_postgres_reader_query_no_rows():
    """Query mode: empty result set → all columns default to nullable (conservative)."""
    from shelfard.tools.postgres.reader import PostgresReader

    desc = [_PgCol("id", 23, None, 4, None, None, None)]
    mock_pg, _ = _make_pg_mock([[], [(23, "int4")]], description=desc)
    with patch("shelfard.tools.postgres.reader.psycopg2", mock_pg):
        result = PostgresReader(
            "postgresql://localhost/test", "empty_table",
            query="SELECT id FROM empty_table",
        ).get_schema()

    assert result.success, result.error
    cols = result.data["schema"]["columns"]
    assert cols[0]["nullable"] == True  # no data → conservative nullable

test("PostgresReader query mode: empty result → all columns nullable (conservative)", test_postgres_reader_query_no_rows)


def test_postgres_reader_no_table_or_query():
    """Missing both table and query → error ToolResult."""
    from shelfard.tools.postgres.reader import PostgresReader

    mock_pg, _ = _make_pg_mock(None)
    with patch("shelfard.tools.postgres.reader.psycopg2", mock_pg):
        result = PostgresReader("postgresql://localhost/test", "x").get_schema()

    assert not result.success
    assert "table" in result.error.lower() or "query" in result.error.lower()

test("PostgresReader: missing table and query → error", test_postgres_reader_no_table_or_query)


def test_postgres_reader_missing_psycopg2():
    """When psycopg2 is not installed (None), get_schema returns a clear error."""
    from shelfard.tools.postgres.reader import PostgresReader

    with patch("shelfard.tools.postgres.reader.psycopg2", None):
        result = PostgresReader("postgresql://localhost/test", "x", table="x").get_schema()
        list_result = PostgresReader("postgresql://localhost/test", "x").list_tables()

    assert not result.success
    assert "psycopg2" in result.error
    assert not list_result.success
    assert "psycopg2" in list_result.error

test("PostgresReader: missing psycopg2 → informative error", test_postgres_reader_missing_psycopg2)


def test_postgres_list_tables():
    """list_tables returns table names from information_schema.tables."""
    from shelfard.tools.postgres.reader import PostgresReader

    table_rows = [("orders", "BASE TABLE"), ("v_summary", "VIEW")]
    mock_pg, _ = _make_pg_mock([table_rows])
    with patch("shelfard.tools.postgres.reader.psycopg2", mock_pg):
        result = PostgresReader("postgresql://localhost/test", "").list_tables()

    assert result.success, result.error
    tables = {t["name"]: t for t in result.data["tables"]}
    assert result.data["count"] == 2
    assert "orders"    in tables
    assert "v_summary" in tables
    assert tables["orders"]["type"]    == "BASE TABLE"
    assert tables["v_summary"]["type"] == "VIEW"

test("PostgresReader.list_tables returns tables and views", test_postgres_list_tables)


# ─────────────────────────────────────────────
# PostgresCheckerConfig
# ─────────────────────────────────────────────

section("PostgresCheckerConfig")


def test_postgres_checker_config_roundtrip():
    config = PostgresCheckerConfig(
        schema_name="orders",
        dsn="postgresql://user:$PG_PASS@host/db",
        env=["PG_PASS"],
        table="orders",
        db_schema="public",
    )
    d = config.to_dict()
    assert d["checker_type"] == "postgres"
    assert d["table"] == "orders"
    assert d["query"] is None

    restored = PostgresCheckerConfig.from_dict(d)
    assert restored.schema_name == "orders"
    assert restored.dsn == "postgresql://user:$PG_PASS@host/db"
    assert restored.env == ["PG_PASS"]
    assert restored.table == "orders"
    assert restored.query is None
    assert restored.sample_size == 100
    assert restored.db_schema == "public"

test("PostgresCheckerConfig roundtrip: to_dict → from_dict", test_postgres_checker_config_roundtrip)


def test_postgres_checker_config_query_mode():
    config = PostgresCheckerConfig(
        schema_name="report",
        dsn="postgresql://user:$PG_PASS@host/db",
        env=["PG_PASS"],
        query="SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
        sample_size=50,
    )
    d = config.to_dict()
    restored = PostgresCheckerConfig.from_dict(d)
    assert restored.table is None
    assert restored.query == config.query
    assert restored.sample_size == 50

test("PostgresCheckerConfig query mode serialises correctly", test_postgres_checker_config_query_mode)


def test_postgres_checker_register_and_get():
    """Register a postgres checker in the registry and retrieve it."""
    with tempfile.TemporaryDirectory() as tmp:
        r = LocalFileRegistry(tmp)
        config = PostgresCheckerConfig(
            schema_name="pg_orders",
            dsn="postgresql://user:$PG_PASS@host/db",
            env=["PG_PASS"],
            table="orders",
        )
        reg = r.register_checker("pg_orders", config)
        assert reg.success

        get = r.get_checker("pg_orders")
        assert get.success
        c = get.data["checker"]
        assert c["checker_type"] == "postgres"
        assert c["dsn"] == "postgresql://user:$PG_PASS@host/db"
        assert c["table"] == "orders"
        assert "PG_PASS" in c["env"]

test("register_checker + get_checker for postgres type", test_postgres_checker_register_and_get)


def test_postgres_checker_run_missing_env():
    """run_checker dispatches to PostgresChecker; missing env var returns informative error."""
    with tempfile.TemporaryDirectory() as tmp:
        r = LocalFileRegistry(tmp)
        config = PostgresCheckerConfig(
            schema_name="pg_test",
            dsn="postgresql://user:$SHELFARD_TEST_PG_PASS@host/db",
            env=["SHELFARD_TEST_PG_PASS"],
            table="orders",
        )
        r.register_checker("pg_test", config)

        # Ensure the env var is not set
        env_backup = os.environ.pop("SHELFARD_TEST_PG_PASS", None)
        try:
            result = r.run_checker("pg_test")
            assert not result.success
            assert "SHELFARD_TEST_PG_PASS" in result.error
        finally:
            if env_backup is not None:
                os.environ["SHELFARD_TEST_PG_PASS"] = env_backup

test("run_checker postgres: dispatches to PostgresChecker, missing env → error", test_postgres_checker_run_missing_env)


# ─────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────

total = passed + failed
print(f"\n{'='*50}")
print(f"Results: {passed}/{total} passed", end="")
if failed:
    print(f"  ({failed} failed)")
    print("\nFailures:")
    for name, err in errors:
        print(f"\n  ✗ {name}")
        print(f"    {err}")
else:
    print(" ✓")
print('='*50)
sys.exit(0 if failed == 0 else 1)
