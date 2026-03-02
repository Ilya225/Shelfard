"""
Unit tests for document parsers (JSON file reader) and STRUCT type inference.

Run: conda run -n shelfard python3 tests/parsers_tests.py
"""

import json
import sys
import tempfile
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from shelfard import (
    ColumnSchema, TableSchema, ColumnType,
    LocalFileRegistry,
    get_registered_schema,
    infer_schema_from_json_file, read_and_register_json_file,
    compare_schemas,
)
from shelfard.models import ChangeType, ChangeSeverity
import shelfard.registry as registry

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
# JSON File Reader
# ─────────────────────────────────────────────

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
        registry._default._root = Path(tmp)
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


# ─────────────────────────────────────────────
# STRUCT Type — Nested Schema
# ─────────────────────────────────────────────

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
        registry._default._root = Path(tmp)
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
