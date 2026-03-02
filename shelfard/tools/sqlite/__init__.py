"""
SQLite schema reader.

Introspects SQLite database files using PRAGMA table_info().
"""

import sqlite3
import os
from datetime import datetime

from ..base import SchemaReader
from ...models import ColumnSchema, ColumnType, TableSchema, ToolResult
from ...type_normalizer import extract_length


_TYPE_MAP: dict[str, ColumnType] = {
    "integer":   ColumnType.INTEGER,
    "int":       ColumnType.INTEGER,
    "tinyint":   ColumnType.INTEGER,
    "smallint":  ColumnType.INTEGER,
    "mediumint": ColumnType.INTEGER,
    "bigint":    ColumnType.BIGINT,
    "real":      ColumnType.FLOAT,
    "double":    ColumnType.FLOAT,
    "float":     ColumnType.FLOAT,
    "numeric":   ColumnType.DECIMAL,
    "decimal":   ColumnType.DECIMAL,
    "text":      ColumnType.TEXT,
    "varchar":   ColumnType.VARCHAR,
    "char":      ColumnType.VARCHAR,
    "blob":      ColumnType.UNKNOWN,
    "boolean":   ColumnType.BOOLEAN,
    "bool":      ColumnType.BOOLEAN,
    "date":      ColumnType.DATE,
    "datetime":  ColumnType.TIMESTAMP,
    "timestamp": ColumnType.TIMESTAMP,
    "json":      ColumnType.JSON,
}


def _normalize_type(raw_type: str) -> ColumnType:
    cleaned = raw_type.lower().strip()
    if "(" in cleaned:
        cleaned = cleaned[:cleaned.index("(")].strip()
    return _TYPE_MAP.get(cleaned, ColumnType.UNKNOWN)


class SQLiteReader(SchemaReader):
    """
    Reads schemas from a SQLite database file.
    Both the database path and target table are provided at construction.
    """

    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name

    def get_schema(self) -> ToolResult:
        """
        Introspects the configured SQLite table and returns its normalized schema.

        Uses PRAGMA table_info() which returns:
        cid | name | type | notnull | dflt_value | pk
        """
        table_name = self.table_name
        if not os.path.exists(self.db_path):
            return ToolResult(success=False, error=f"Database file not found: {self.db_path}")

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Verify table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if not cursor.fetchone():
                conn.close()
                return ToolResult(
                    success=False,
                    error=f"Table '{table_name}' not found in {self.db_path}",
                    next_action_hint="Call list_sqlite_tables() to see available tables."
                )

            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            rows = cursor.fetchall()
            conn.close()

            columns = []
            for row in rows:
                cid, name, raw_type, notnull, default_val, is_pk = row
                col_type = _normalize_type(raw_type or "text")
                max_length = extract_length(raw_type or "")

                columns.append(ColumnSchema(
                    name=name,
                    col_type=col_type,
                    nullable=not bool(notnull),
                    max_length=max_length,
                    default_value=str(default_val) if default_val is not None else None,
                ))

            schema = TableSchema(
                table_name=table_name,
                columns=columns,
                source="sqlite",
                captured_at=datetime.utcnow().isoformat(),
            )

            return ToolResult(
                success=True,
                data={"schema": schema.to_dict()},
                next_action_hint="Now call get_registered_schema() and compare_schemas() to detect drift."
            )

        except Exception as e:
            return ToolResult(success=False, error=f"SQLite introspection failed: {e}")

    def list_tables(self) -> ToolResult:
        """Lists all user tables in the SQLite database."""
        if not os.path.exists(self.db_path):
            return ToolResult(success=False, error=f"Database file not found: {self.db_path}")

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

            return ToolResult(
                success=True,
                data={"tables": tables, "count": len(tables)},
                next_action_hint="Call get_sqlite_schema() for any table you want to inspect."
            )
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to list tables: {e}")


# ── Backward-compatible module-level wrappers ─────────────────────────────

def get_sqlite_schema(db_path: str, table_name: str) -> ToolResult:
    return SQLiteReader(db_path, table_name).get_schema()


def list_sqlite_tables(db_path: str) -> ToolResult:
    return SQLiteReader(db_path, "").list_tables()
