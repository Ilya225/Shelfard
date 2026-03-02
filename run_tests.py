"""
Standalone test runner — no pytest needed.
Run: python3 run_tests.py

Covers basic pre-registry tests: SQLite introspection, schema comparison,
type normalization, severity roll-up, and pure STRUCT drift detection.

Domain-specific tests live in:
  tests/registry_tests.py    — schema registry + consumer subscriptions
  tests/parsers_tests.py     — JSON file reader + STRUCT inference
  tests/rest_tests.py        — REST endpoint reader (mock HTTP server)
  tests/postgresql_tests.py  — PostgreSQL reader + checker (mocked psycopg2)
"""

import sys
import traceback
import sqlite3
import tempfile
from pathlib import Path

# Make tools importable
sys.path.insert(0, str(Path(__file__).parent))

from shelfard import (
    ColumnSchema, TableSchema, ColumnType, ChangeSeverity, ChangeType,
    get_sqlite_schema, compare_schemas,
)

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

def make_schema(table_name, columns):
    return TableSchema(table_name=table_name, columns=columns, source="test")


# ─────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────

section("SQLite Introspection")

def test_basic_introspection():
    with tempfile.TemporaryDirectory() as tmp:
        db = f"{tmp}/test.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE users (id INTEGER NOT NULL, email VARCHAR(255), age INTEGER)")
        conn.commit(); conn.close()
        result = get_sqlite_schema(db, "users")
        assert result.success, result.error
        schema = result.data["schema"]
        assert schema["table_name"] == "users"
        assert len(schema["columns"]) == 3
        assert schema["columns"][0]["name"] == "id"

test("basic table introspection", test_basic_introspection)

def test_type_normalization():
    with tempfile.TemporaryDirectory() as tmp:
        db = f"{tmp}/test.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE t (a INTEGER, b REAL, c TEXT, d BOOLEAN, e TIMESTAMP, f NUMERIC)")
        conn.commit(); conn.close()
        result = get_sqlite_schema(db, "t")
        assert result.success
        types = {c["name"]: c["col_type"] for c in result.data["schema"]["columns"]}
        assert types["a"] == "integer"
        assert types["b"] == "float"
        assert types["c"] == "text"
        assert types["d"] == "boolean"
        assert types["e"] == "timestamp"
        assert types["f"] == "decimal"

test("type normalization", test_type_normalization)

def test_nonexistent_db():
    result = get_sqlite_schema("/nonexistent/path.db", "any")
    assert not result.success
    assert "not found" in result.error.lower()

test("nonexistent db returns error", test_nonexistent_db)

def test_nonexistent_table():
    with tempfile.TemporaryDirectory() as tmp:
        db = f"{tmp}/test.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE real_table (id INTEGER)")
        conn.commit(); conn.close()
        result = get_sqlite_schema(db, "ghost_table")
        assert not result.success
        assert result.next_action_hint is not None

test("nonexistent table returns error with hint", test_nonexistent_table)

def test_varchar_length_captured():
    with tempfile.TemporaryDirectory() as tmp:
        db = f"{tmp}/test.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE t (name VARCHAR(100))")
        conn.commit(); conn.close()
        result = get_sqlite_schema(db, "t")
        assert result.success
        col = result.data["schema"]["columns"][0]
        assert col["max_length"] == 100

test("varchar length is captured", test_varchar_length_captured)


section("Schema Comparison — No Changes")

def test_identical_schemas():
    schema = make_schema("orders", [
        ColumnSchema("order_id",    ColumnType.INTEGER,   nullable=False),
        ColumnSchema("customer_id", ColumnType.INTEGER,   nullable=False),
        ColumnSchema("amount",      ColumnType.DECIMAL,   nullable=True, precision=18, scale=4),
        ColumnSchema("status",      ColumnType.VARCHAR,   nullable=True, max_length=50),
        ColumnSchema("created_at",  ColumnType.TIMESTAMP, nullable=False),
    ])
    result = compare_schemas(schema, schema)
    assert result.success
    diff = result.data["diff"]
    assert len(diff["changes"]) == 0
    assert diff["overall_severity"] == ChangeSeverity.SAFE

test("identical schemas produce no changes", test_identical_schemas)


section("Schema Comparison — Column Additions")

def make_orders_v1():
    return make_schema("orders", [
        ColumnSchema("order_id",    ColumnType.INTEGER,   nullable=False),
        ColumnSchema("customer_id", ColumnType.INTEGER,   nullable=False),
        ColumnSchema("amount",      ColumnType.DECIMAL,   nullable=True, precision=18, scale=4),
        ColumnSchema("status",      ColumnType.VARCHAR,   nullable=True, max_length=50),
        ColumnSchema("created_at",  ColumnType.TIMESTAMP, nullable=False),
    ])

def test_nullable_column_added_is_safe():
    old = make_orders_v1()
    new = TableSchema(
        table_name="orders",
        columns=old.columns + [ColumnSchema("discount_pct", ColumnType.DECIMAL, nullable=True)],
        source="test"
    )
    result = compare_schemas(old, new)
    diff = result.data["diff"]
    assert len(diff["changes"]) == 1
    assert diff["changes"][0]["change_type"] == ChangeType.COLUMN_ADDED
    assert diff["changes"][0]["severity"] == ChangeSeverity.SAFE
    assert diff["overall_severity"] == ChangeSeverity.SAFE

test("nullable column addition is SAFE", test_nullable_column_added_is_safe)

def test_not_null_no_default_is_breaking():
    old = make_orders_v1()
    new = TableSchema(
        table_name="orders",
        columns=old.columns + [ColumnSchema("required_field", ColumnType.INTEGER, nullable=False, default_value=None)],
        source="test"
    )
    result = compare_schemas(old, new)
    diff = result.data["diff"]
    assert diff["changes"][0]["severity"] == ChangeSeverity.BREAKING
    assert diff["overall_severity"] == ChangeSeverity.BREAKING

test("NOT NULL no default addition is BREAKING", test_not_null_no_default_is_breaking)

def test_not_null_with_default_is_safe():
    old = make_orders_v1()
    new = TableSchema(
        table_name="orders",
        columns=old.columns + [ColumnSchema("version", ColumnType.INTEGER, nullable=False, default_value="1")],
        source="test"
    )
    result = compare_schemas(old, new)
    diff = result.data["diff"]
    assert diff["changes"][0]["severity"] == ChangeSeverity.SAFE

test("NOT NULL with default addition is SAFE", test_not_null_with_default_is_safe)


section("Schema Comparison — Column Removals")

def test_removal_always_breaking():
    old = make_orders_v1()
    new_cols = [c for c in old.columns if c.name != "status"]
    new = TableSchema(table_name="orders", columns=new_cols, source="test")
    result = compare_schemas(old, new)
    diff = result.data["diff"]
    removed = [c for c in diff["changes"] if c["change_type"] == ChangeType.COLUMN_REMOVED]
    assert len(removed) == 1
    assert removed[0]["severity"] == ChangeSeverity.BREAKING

test("column removal is always BREAKING", test_removal_always_breaking)


section("Schema Comparison — Type Changes")

def test_int_to_bigint_safe():
    old = make_schema("t", [ColumnSchema("id", ColumnType.INTEGER)])
    new = make_schema("t", [ColumnSchema("id", ColumnType.BIGINT)])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["changes"][0]["change_type"] == ChangeType.TYPE_WIDENED
    assert diff["changes"][0]["severity"] == ChangeSeverity.SAFE

test("integer → bigint is SAFE widening", test_int_to_bigint_safe)

def test_varchar_widening_safe():
    old = make_schema("t", [ColumnSchema("name", ColumnType.VARCHAR, max_length=50)])
    new = make_schema("t", [ColumnSchema("name", ColumnType.VARCHAR, max_length=200)])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["changes"][0]["severity"] == ChangeSeverity.SAFE

test("varchar(50) → varchar(200) is SAFE", test_varchar_widening_safe)

def test_varchar_narrowing_breaking():
    old = make_schema("t", [ColumnSchema("code", ColumnType.VARCHAR, max_length=100)])
    new = make_schema("t", [ColumnSchema("code", ColumnType.VARCHAR, max_length=10)])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["changes"][0]["severity"] == ChangeSeverity.BREAKING

test("varchar(100) → varchar(10) is BREAKING", test_varchar_narrowing_breaking)

def test_integer_to_varchar_breaking():
    old = make_schema("t", [ColumnSchema("amount", ColumnType.INTEGER)])
    new = make_schema("t", [ColumnSchema("amount", ColumnType.VARCHAR)])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["changes"][0]["severity"] == ChangeSeverity.BREAKING

test("integer → varchar is BREAKING", test_integer_to_varchar_breaking)

def test_varchar_to_text_safe():
    old = make_schema("t", [ColumnSchema("notes", ColumnType.VARCHAR, max_length=500)])
    new = make_schema("t", [ColumnSchema("notes", ColumnType.TEXT)])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["changes"][0]["severity"] == ChangeSeverity.SAFE

test("varchar → text is SAFE", test_varchar_to_text_safe)


section("Schema Comparison — Nullability")

def test_not_null_to_nullable_safe():
    old = make_schema("t", [ColumnSchema("code", ColumnType.VARCHAR, nullable=False)])
    new = make_schema("t", [ColumnSchema("code", ColumnType.VARCHAR, nullable=True)])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["changes"][0]["change_type"] == ChangeType.NULLABILITY_RELAXED
    assert diff["changes"][0]["severity"] == ChangeSeverity.SAFE

test("NOT NULL → NULL is SAFE (relaxed)", test_not_null_to_nullable_safe)

def test_nullable_to_not_null_breaking():
    old = make_schema("t", [ColumnSchema("code", ColumnType.VARCHAR, nullable=True)])
    new = make_schema("t", [ColumnSchema("code", ColumnType.VARCHAR, nullable=False)])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["changes"][0]["change_type"] == ChangeType.NULLABILITY_TIGHTENED
    assert diff["changes"][0]["severity"] == ChangeSeverity.BREAKING

test("NULL → NOT NULL is BREAKING (tightened)", test_nullable_to_not_null_breaking)


section("Schema Comparison — Reordering & Defaults")

def test_reorder_is_warning():
    old = make_schema("t", [
        ColumnSchema("a", ColumnType.INTEGER),
        ColumnSchema("b", ColumnType.INTEGER),
        ColumnSchema("c", ColumnType.INTEGER),
    ])
    new = make_schema("t", [
        ColumnSchema("c", ColumnType.INTEGER),
        ColumnSchema("a", ColumnType.INTEGER),
        ColumnSchema("b", ColumnType.INTEGER),
    ])
    diff = compare_schemas(old, new).data["diff"]
    reorders = [c for c in diff["changes"] if c["change_type"] == ChangeType.COLUMN_REORDERED]
    assert len(reorders) == 1
    assert reorders[0]["severity"] == ChangeSeverity.WARNING
    assert "positional" in reorders[0]["reasoning"].lower()

test("column reorder is WARNING with positional access note", test_reorder_is_warning)

def test_default_change_is_warning():
    old = make_schema("t", [ColumnSchema("status", ColumnType.VARCHAR, default_value="pending")])
    new = make_schema("t", [ColumnSchema("status", ColumnType.VARCHAR, default_value="active")])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["changes"][0]["change_type"] == ChangeType.DEFAULT_CHANGED
    assert diff["changes"][0]["severity"] == ChangeSeverity.WARNING

test("default value change is WARNING", test_default_change_is_warning)


section("Severity Roll-up")

def test_worst_case_wins():
    old = make_schema("t", [
        ColumnSchema("safe_col",     ColumnType.INTEGER, nullable=True),
        ColumnSchema("breaking_col", ColumnType.INTEGER, nullable=False),
    ])
    new = make_schema("t", [
        ColumnSchema("safe_col", ColumnType.BIGINT, nullable=True),
        # breaking_col removed
    ])
    diff = compare_schemas(old, new).data["diff"]
    assert diff["overall_severity"] == ChangeSeverity.BREAKING

test("overall severity = worst change severity", test_worst_case_wins)


section("Real-world Scenario: SaaS Source Update")

def test_saas_schema_update():
    old = make_schema("subscriptions", [
        ColumnSchema("sub_id",     ColumnType.INTEGER,   nullable=False),
        ColumnSchema("plan",       ColumnType.VARCHAR,   nullable=False, max_length=50),
        ColumnSchema("mrr",        ColumnType.DECIMAL,   nullable=True,  precision=10),
        ColumnSchema("created_at", ColumnType.TIMESTAMP, nullable=False),
    ])
    new = make_schema("subscriptions", [
        ColumnSchema("subscription_id", ColumnType.INTEGER,   nullable=False),  # rename = remove+add → BREAKING
        ColumnSchema("plan",            ColumnType.VARCHAR,   nullable=False, max_length=50),
        ColumnSchema("mrr",             ColumnType.DECIMAL,   nullable=True,  precision=18),  # widened → SAFE
        ColumnSchema("created_at",      ColumnType.TIMESTAMP, nullable=False),
        ColumnSchema("trial_ends_at",   ColumnType.TIMESTAMP, nullable=True),   # new → SAFE
        ColumnSchema("seats",           ColumnType.INTEGER,   nullable=True),   # new → SAFE
    ])
    result = compare_schemas(old, new)
    assert result.success
    diff = result.data["diff"]
    assert diff["overall_severity"] == ChangeSeverity.BREAKING

    removed = [c for c in diff["changes"] if c["change_type"] == ChangeType.COLUMN_REMOVED]
    added   = [c for c in diff["changes"] if c["change_type"] == ChangeType.COLUMN_ADDED]
    widened = [c for c in diff["changes"] if c["change_type"] == ChangeType.TYPE_WIDENED]

    assert any(c["column_name"] == "sub_id" for c in removed)
    assert any(c["column_name"] == "subscription_id" for c in added)
    assert any(c["column_name"] == "trial_ends_at" for c in added)
    assert any(c["column_name"] == "mrr" for c in widened)

    print(f"\n    Summary: {diff['summary']}")
    for c in diff["changes"]:
        print(f"    [{c['severity']:8}] {c['change_type']:25} '{c['column_name']}'")

test("SaaS rename + new columns + widening detected correctly", test_saas_schema_update)


section("STRUCT Drift — Pure Comparison")

def test_struct_drift_field_added():
    old = TableSchema("t", [
        ColumnSchema("address", ColumnType.STRUCT, fields=[
            ColumnSchema("street", ColumnType.VARCHAR),
        ])
    ], source="test")
    new = TableSchema("t", [
        ColumnSchema("address", ColumnType.STRUCT, fields=[
            ColumnSchema("street", ColumnType.VARCHAR),
            ColumnSchema("zip", ColumnType.VARCHAR, nullable=True),
        ])
    ], source="test")
    diff = compare_schemas(old, new).data["diff"]
    changes = {c["column_name"]: c for c in diff["changes"]}
    assert "address.zip" in changes
    assert changes["address.zip"]["change_type"] == ChangeType.COLUMN_ADDED
    assert changes["address.zip"]["severity"]    == ChangeSeverity.SAFE

test("drift: nullable field added to nested STRUCT → SAFE with qualified name", test_struct_drift_field_added)

def test_struct_drift_field_removed():
    old = TableSchema("t", [
        ColumnSchema("address", ColumnType.STRUCT, fields=[
            ColumnSchema("street", ColumnType.VARCHAR),
            ColumnSchema("zip", ColumnType.VARCHAR),
        ])
    ], source="test")
    new = TableSchema("t", [
        ColumnSchema("address", ColumnType.STRUCT, fields=[
            ColumnSchema("street", ColumnType.VARCHAR),
        ])
    ], source="test")
    diff = compare_schemas(old, new).data["diff"]
    changes = {c["column_name"]: c for c in diff["changes"]}
    assert "address.zip" in changes
    assert changes["address.zip"]["change_type"] == ChangeType.COLUMN_REMOVED
    assert changes["address.zip"]["severity"]    == ChangeSeverity.BREAKING

test("drift: field removed from nested STRUCT → BREAKING with qualified name", test_struct_drift_field_removed)

def test_json_to_struct_is_breaking():
    old = TableSchema("t", [ColumnSchema("meta", ColumnType.JSON)], source="test")
    new = TableSchema("t", [
        ColumnSchema("meta", ColumnType.STRUCT, fields=[ColumnSchema("k", ColumnType.VARCHAR)])
    ], source="test")
    diff = compare_schemas(old, new).data["diff"]
    assert diff["overall_severity"] == ChangeSeverity.BREAKING
    assert any(c["change_type"] == ChangeType.TYPE_CHANGED for c in diff["changes"])

test("JSON → STRUCT is a BREAKING type change", test_json_to_struct_is_breaking)

def test_qualified_names_in_nested_diff():
    old = TableSchema("t", [
        ColumnSchema("user", ColumnType.STRUCT, fields=[
            ColumnSchema("address", ColumnType.STRUCT, fields=[
                ColumnSchema("zip", ColumnType.VARCHAR),
            ])
        ])
    ], source="test")
    new = TableSchema("t", [
        ColumnSchema("user", ColumnType.STRUCT, fields=[
            ColumnSchema("address", ColumnType.STRUCT, fields=[
                ColumnSchema("zip", ColumnType.INTEGER),   # type changed
            ])
        ])
    ], source="test")
    diff = compare_schemas(old, new).data["diff"]
    assert len(diff["changes"]) == 1
    assert diff["changes"][0]["column_name"] == "user.address.zip"
    assert diff["changes"][0]["severity"] == ChangeSeverity.BREAKING

test("3-level nested drift uses fully qualified name 'user.address.zip'", test_qualified_names_in_nested_diff)


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
