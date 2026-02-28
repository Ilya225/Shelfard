"""
Standalone test runner — no pytest needed.
Run: python3 run_tests.py
"""

import sys
import traceback
import sqlite3
import tempfile
import os
import json
from pathlib import Path

# Make tools importable
sys.path.insert(0, str(Path(__file__).parent))

from tools import (
    ColumnSchema, TableSchema, ColumnType, ChangeSeverity, ChangeType,
    get_sqlite_schema, register_schema, get_registered_schema,
    compare_schemas, compare_schemas_from_dicts, get_schema_from_json,
    infer_schema_from_json_file, read_and_register_json_file,
)
import tools.registry as registry

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


section("Schema Registry")

def make_orders_v1():
    return TableSchema(
        table_name="orders",
        columns=[
            ColumnSchema("order_id",    ColumnType.INTEGER,   nullable=False),
            ColumnSchema("customer_id", ColumnType.INTEGER,   nullable=False),
            ColumnSchema("amount",      ColumnType.DECIMAL,   nullable=True, precision=18, scale=4),
            ColumnSchema("status",      ColumnType.VARCHAR,   nullable=True, max_length=50),
            ColumnSchema("created_at",  ColumnType.TIMESTAMP, nullable=False),
        ],
        source="test"
    )

def test_register_and_retrieve():
    with tempfile.TemporaryDirectory() as tmp:
        registry.REGISTRY_DIR = Path(tmp)
        schema = make_orders_v1()
        reg = register_schema("orders", schema)
        assert reg.success
        get = get_registered_schema("orders")
        assert get.success
        assert get.data["schema"]["table_name"] == "orders"
        assert len(get.data["schema"]["columns"]) == 5

test("register and retrieve schema", test_register_and_retrieve)

def test_unregistered_table():
    with tempfile.TemporaryDirectory() as tmp:
        registry.REGISTRY_DIR = Path(tmp)
        result = get_registered_schema("nonexistent")
        assert not result.success
        assert result.next_action_hint is not None

test("unregistered table returns helpful error", test_unregistered_table)

def test_multiple_versions():
    with tempfile.TemporaryDirectory() as tmp:
        registry.REGISTRY_DIR = Path(tmp)
        v1 = make_orders_v1()
        register_schema("orders", v1)
        v2 = TableSchema(
            table_name="orders",
            columns=v1.columns + [ColumnSchema("notes", ColumnType.TEXT, nullable=True)],
            source="test"
        )
        register_schema("orders", v2)
        result = get_registered_schema("orders", version="latest")
        assert result.success
        assert len(result.data["schema"]["columns"]) == 6

test("multiple versions — latest returns newest", test_multiple_versions)


section("Schema Comparison — No Changes")

def test_identical_schemas():
    schema = make_orders_v1()
    result = compare_schemas(schema, schema)
    assert result.success
    diff = result.data["diff"]
    assert len(diff["changes"]) == 0
    assert diff["overall_severity"] == ChangeSeverity.SAFE

test("identical schemas produce no changes", test_identical_schemas)


section("Schema Comparison — Column Additions")

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


section("End-to-End: SQLite → Registry → Compare")

def test_full_pipeline():
    with tempfile.TemporaryDirectory() as tmp:
        registry.REGISTRY_DIR = Path(tmp)
        db = f"{tmp}/events.db"

        # Create v1
        conn = sqlite3.connect(db)
        conn.execute("""
            CREATE TABLE events (
                event_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                payload TEXT,
                occurred_at TIMESTAMP NOT NULL
            )
        """)
        conn.commit(); conn.close()

        # Introspect and register
        v1_result = get_sqlite_schema(db, "events")
        assert v1_result.success

        raw = v1_result.data["schema"]
        v1_schema = TableSchema(
            table_name=raw["table_name"],
            columns=[ColumnSchema(
                name=c["name"],
                col_type=ColumnType(c["col_type"]),
                nullable=c["nullable"],
                max_length=c.get("max_length"),
            ) for c in raw["columns"]],
            source="sqlite"
        )
        assert register_schema("events", v1_schema).success

        # v2 schema arrives as JSON (simulating Kafka or API payload)
        v2_dict = {
            "table_name": "events",
            "columns": [
                {"name": "event_id",   "col_type": "integer",   "nullable": False},
                {"name": "user_id",    "col_type": "integer",   "nullable": False},
                {"name": "event_type", "col_type": "varchar",   "nullable": False, "max_length": 100},
                {"name": "payload",    "col_type": "json",      "nullable": True},    # text→json: BREAKING
                {"name": "occurred_at","col_type": "timestamp", "nullable": False},
                {"name": "session_id", "col_type": "varchar",   "nullable": True, "max_length": 64},  # new: SAFE
            ]
        }

        registered = get_registered_schema("events")
        assert registered.success

        diff_result = compare_schemas_from_dicts(registered.data["schema"], v2_dict)
        assert diff_result.success

        diff = diff_result.data["diff"]
        change_types = [c["change_type"] for c in diff["changes"]]

        assert ChangeType.COLUMN_ADDED in change_types    # session_id
        assert ChangeType.TYPE_CHANGED in change_types    # payload: text → json
        assert diff["overall_severity"] == ChangeSeverity.BREAKING

        print(f"\n    {diff['summary']}")

test("full pipeline: introspect → register → detect drift", test_full_pipeline)


section("JSON File Reader")

def test_basic_type_inference():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/payload.json"
        with open(path, "w") as f:
            json.dump({
                "count": 42,
                "ratio": 3.14,
                "active": True,
                "label": "hello",
                "tags": ["a", "b"],
                "meta": {"k": "v"},
                "deleted_at": None,
            }, f)
        result = infer_schema_from_json_file(path, "payload")
        assert result.success, result.error
        cols = {c["name"]: c for c in result.data["schema"]["columns"]}
        assert cols["count"]["col_type"]      == ColumnType.INTEGER
        assert cols["ratio"]["col_type"]      == ColumnType.FLOAT
        assert cols["active"]["col_type"]     == ColumnType.BOOLEAN
        assert cols["label"]["col_type"]      == ColumnType.VARCHAR
        assert cols["tags"]["col_type"]       == ColumnType.ARRAY
        assert cols["meta"]["col_type"]       == ColumnType.STRUCT
        assert cols["deleted_at"]["col_type"] == ColumnType.UNKNOWN

test("basic type inference (int/float/bool/str/list/dict/null)", test_basic_type_inference)

def test_datetime_string_detection():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/ts.json"
        with open(path, "w") as f:
            json.dump({"created_at": "2024-01-15T10:30:00"}, f)
        result = infer_schema_from_json_file(path, "ts")
        assert result.success, result.error
        col = result.data["schema"]["columns"][0]
        assert col["col_type"] == ColumnType.TIMESTAMP

test("datetime string → TIMESTAMP", test_datetime_string_detection)

def test_date_string_detection():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/dt.json"
        with open(path, "w") as f:
            json.dump({"birth_date": "2024-01-15"}, f)
        result = infer_schema_from_json_file(path, "dt")
        assert result.success, result.error
        col = result.data["schema"]["columns"][0]
        assert col["col_type"] == ColumnType.DATE

test("date string → DATE", test_date_string_detection)

def test_nullable_inference():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/null.json"
        with open(path, "w") as f:
            json.dump({"present": "value", "absent": None}, f)
        result = infer_schema_from_json_file(path, "null")
        assert result.success, result.error
        cols = {c["name"]: c for c in result.data["schema"]["columns"]}
        assert cols["present"]["nullable"] == False
        assert cols["absent"]["nullable"]  == True

test("null field → nullable=True, non-null → nullable=False", test_nullable_inference)

def test_bool_before_int():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/bool.json"
        with open(path, "w") as f:
            json.dump({"flag_true": True, "flag_false": False}, f)
        result = infer_schema_from_json_file(path, "bool")
        assert result.success, result.error
        cols = {c["name"]: c for c in result.data["schema"]["columns"]}
        assert cols["flag_true"]["col_type"]  == ColumnType.BOOLEAN
        assert cols["flag_false"]["col_type"] == ColumnType.BOOLEAN

test("bool values → BOOLEAN (not INTEGER)", test_bool_before_int)

def test_root_level_array():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/arr.json"
        with open(path, "w") as f:
            json.dump([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], f)
        result = infer_schema_from_json_file(path, "arr")
        assert result.success, result.error
        cols = {c["name"]: c for c in result.data["schema"]["columns"]}
        assert cols["id"]["col_type"]   == ColumnType.INTEGER
        assert cols["name"]["col_type"] == ColumnType.VARCHAR

test("root-level array → uses first element", test_root_level_array)

def test_file_not_found():
    result = infer_schema_from_json_file("/nonexistent/payload.json", "x")
    assert not result.success
    assert "not found" in result.error.lower()

test("file not found → ToolResult(success=False)", test_file_not_found)

def test_read_and_register():
    with tempfile.TemporaryDirectory() as tmp:
        registry_dir = Path(tmp) / "registry"
        registry_dir.mkdir()
        registry.REGISTRY_DIR = registry_dir
        path = f"{tmp}/api_response.json"
        with open(path, "w") as f:
            json.dump({
                "user_id": 99,
                "email": "user@example.com",
                "verified": True,
                "created_at": "2024-03-01T12:00:00",
            }, f)
        reg_result = read_and_register_json_file(path, "api_response")
        assert reg_result.success, reg_result.error

        get_result = get_registered_schema("api_response")
        assert get_result.success
        cols = {c["name"]: c for c in get_result.data["schema"]["columns"]}
        assert cols["user_id"]["col_type"]    == ColumnType.INTEGER
        assert cols["email"]["col_type"]      == ColumnType.VARCHAR
        assert cols["verified"]["col_type"]   == ColumnType.BOOLEAN
        assert cols["created_at"]["col_type"] == ColumnType.TIMESTAMP

test("end-to-end: read JSON file + register + retrieve from registry", test_read_and_register)


section("STRUCT Type — Nested Schema")

def test_nested_object_becomes_struct():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/nested.json"
        with open(path, "w") as f:
            json.dump({"user": {"id": 1, "name": "Alice"}}, f)
        result = infer_schema_from_json_file(path, "nested")
        assert result.success, result.error
        cols = {c["name"]: c for c in result.data["schema"]["columns"]}
        assert cols["user"]["col_type"] == ColumnType.STRUCT
        fields = {f["name"]: f for f in cols["user"]["fields"]}
        assert fields["id"]["col_type"]   == ColumnType.INTEGER
        assert fields["name"]["col_type"] == ColumnType.VARCHAR

test("nested dict → STRUCT with correct child fields", test_nested_object_becomes_struct)

def test_deep_nesting():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/deep.json"
        with open(path, "w") as f:
            json.dump({"a": {"b": {"c": 42}}}, f)
        result = infer_schema_from_json_file(path, "deep")
        assert result.success, result.error
        top = result.data["schema"]["columns"][0]
        assert top["col_type"] == ColumnType.STRUCT
        mid = top["fields"][0]
        assert mid["col_type"] == ColumnType.STRUCT
        leaf = mid["fields"][0]
        assert leaf["col_type"] == ColumnType.INTEGER

test("3-level deep nesting → STRUCT → STRUCT → INTEGER", test_deep_nesting)

def test_struct_field_types():
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/mixed.json"
        with open(path, "w") as f:
            json.dump({"address": {
                "street": "Main St",
                "number": 42,
                "active": True,
                "note": None,
            }}, f)
        result = infer_schema_from_json_file(path, "mixed")
        assert result.success, result.error
        struct_col = result.data["schema"]["columns"][0]
        fields = {f["name"]: f for f in struct_col["fields"]}
        assert fields["street"]["col_type"] == ColumnType.VARCHAR
        assert fields["number"]["col_type"] == ColumnType.INTEGER
        assert fields["active"]["col_type"] == ColumnType.BOOLEAN
        assert fields["note"]["col_type"]   == ColumnType.UNKNOWN
        assert fields["note"]["nullable"]   == True

test("mixed types inside STRUCT fields inferred correctly", test_struct_field_types)

def test_struct_roundtrip():
    with tempfile.TemporaryDirectory() as tmp:
        registry_dir = Path(tmp) / "registry"
        registry_dir.mkdir()
        registry.REGISTRY_DIR = registry_dir
        path = f"{tmp}/event.json"
        with open(path, "w") as f:
            json.dump({"payload": {"event_type": "click", "value": 1}}, f)
        assert read_and_register_json_file(path, "event").success

        get_result = get_registered_schema("event")
        assert get_result.success
        cols = {c["name"]: c for c in get_result.data["schema"]["columns"]}
        assert cols["payload"]["col_type"] == ColumnType.STRUCT
        fields = {f["name"]: f for f in cols["payload"]["fields"]}
        assert fields["event_type"]["col_type"] == ColumnType.VARCHAR
        assert fields["value"]["col_type"]      == ColumnType.INTEGER

test("STRUCT round-trip: infer → register → retrieve → fields preserved", test_struct_roundtrip)

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
