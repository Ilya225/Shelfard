"""
PostgreSQL schema reader.

Supports two modes:
  - table mode:  introspect a single table or view via information_schema.columns
  - query mode:  infer schema from a custom SQL query; nullability is sampled
                 from up to ``sample_size`` rows — columns with no NULLs in the
                 sample are marked NOT NULL by contract.

Install the optional dependency to use this reader::

    pip install psycopg2-binary
    # or: pip install shelfard[postgres]
"""

from datetime import datetime, timezone
from typing import Optional

from ...models import ColumnSchema, ColumnType, TableSchema, ToolResult
from ..base import SchemaReader
from ..sql.base import (
    build_columns_from_query_result,
    introspect_table_via_information_schema,
    sample_query,
)

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore


# ── Type map ──────────────────────────────────────────────────────────────────

_TYPE_MAP: dict[str, ColumnType] = {
    "smallint":                      ColumnType.INTEGER,
    "integer":                       ColumnType.INTEGER,
    "int":                           ColumnType.INTEGER,
    "int2":                          ColumnType.INTEGER,
    "int4":                          ColumnType.INTEGER,
    "bigint":                        ColumnType.BIGINT,
    "int8":                          ColumnType.BIGINT,
    "real":                          ColumnType.FLOAT,
    "float4":                        ColumnType.FLOAT,
    "double precision":              ColumnType.FLOAT,
    "float8":                        ColumnType.FLOAT,
    "numeric":                       ColumnType.DECIMAL,
    "decimal":                       ColumnType.DECIMAL,
    "varchar":                       ColumnType.VARCHAR,
    "character varying":             ColumnType.VARCHAR,
    "char":                          ColumnType.VARCHAR,
    "bpchar":                        ColumnType.VARCHAR,   # PostgreSQL internal name
    "text":                          ColumnType.TEXT,
    "boolean":                       ColumnType.BOOLEAN,
    "bool":                          ColumnType.BOOLEAN,
    "date":                          ColumnType.DATE,
    "timestamp":                     ColumnType.TIMESTAMP,
    "timestamptz":                   ColumnType.TIMESTAMP,
    "timestamp without time zone":   ColumnType.TIMESTAMP,
    "timestamp with time zone":      ColumnType.TIMESTAMP,
    "json":                          ColumnType.JSON,
    "jsonb":                         ColumnType.JSON,
    "array":                         ColumnType.ARRAY,
    "_text":                         ColumnType.ARRAY,     # pg array types use _ prefix
    "_int4":                         ColumnType.ARRAY,
    "_int8":                         ColumnType.ARRAY,
}


def _normalize_type(raw_type: str) -> ColumnType:
    cleaned = raw_type.lower().strip()
    if "(" in cleaned:
        cleaned = cleaned[: cleaned.index("(")].strip()
    return _TYPE_MAP.get(cleaned, ColumnType.UNKNOWN)


# ── Reader ────────────────────────────────────────────────────────────────────

class PostgresReader(SchemaReader):
    """
    Reads schema from a PostgreSQL database.

    Two modes (exactly one of ``table`` or ``query`` must be provided):

    *Table mode* — ``table="my_table"``
        Introspects the table or view via ``information_schema.columns``.
        Returns exact types and NOT NULL constraints as declared in the catalog.

    *Query mode* — ``query="SELECT id, name FROM orders WHERE …"``
        Executes the query with ``LIMIT sample_size``, then infers column types
        from the PostgreSQL OID catalog and nullability from the sampled data.
        Columns with zero NULL values in the sample are marked NOT NULL by contract.

    The ``dsn`` must be a fully-resolved connection string (no ``$VAR``
    placeholders).  Callers that need env-var substitution should use
    :class:`~shelfard.tools.postgres.checker.PostgresChecker` instead.
    """

    def __init__(
        self,
        dsn: str,
        schema_name: str,
        *,
        table: Optional[str] = None,
        query: Optional[str] = None,
        db_schema: str = "public",
        sample_size: int = 100,
    ) -> None:
        self.dsn = dsn
        self.schema_name = schema_name
        self.table = table
        self.query = query
        self.db_schema = db_schema
        self.sample_size = sample_size

    def get_schema(self) -> ToolResult:
        if psycopg2 is None:
            return ToolResult(
                success=False,
                error="psycopg2 is not installed. Run: pip install psycopg2-binary",
                next_action_hint="pip install psycopg2-binary  # or: pip install shelfard[postgres]",
            )

        if not self.table and not self.query:
            return ToolResult(
                success=False,
                error="Either table or query must be provided to PostgresReader",
            )
        if self.table and self.query:
            return ToolResult(
                success=False,
                error="Provide either table or query, not both",
            )

        try:
            conn = psycopg2.connect(self.dsn)
        except Exception as e:
            return ToolResult(
                success=False,
                error=f"Failed to connect to PostgreSQL: {e}",
                next_action_hint="Check DSN and ensure the PostgreSQL server is reachable",
            )

        try:
            with conn.cursor() as cur:
                if self.table:
                    columns = self._get_table_columns(cur)
                else:
                    columns = self._get_query_columns(cur)
        except Exception as e:
            conn.close()
            return ToolResult(success=False, error=f"Failed to read schema: {e}")
        finally:
            conn.close()

        if not columns:
            target = self.table or "(custom query)"
            return ToolResult(
                success=False,
                error=(
                    f"No columns found for '{target}' in schema '{self.db_schema}'. "
                    "Check that the table/view exists and --db-schema is correct."
                ),
            )

        schema = TableSchema(
            table_name=self.schema_name,
            columns=columns,
            source="postgresql",
            captured_at=datetime.now(timezone.utc).isoformat(),
        )
        return ToolResult(success=True, data={"schema": schema.to_dict()})

    def list_tables(self) -> ToolResult:
        if psycopg2 is None:
            return ToolResult(
                success=False,
                error="psycopg2 is not installed. Run: pip install psycopg2-binary",
            )

        try:
            conn = psycopg2.connect(self.dsn)
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to connect to PostgreSQL: {e}")

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_name
                    """,
                    (self.db_schema,),
                )
                rows = cur.fetchall()
        except Exception as e:
            conn.close()
            return ToolResult(success=False, error=f"Failed to list tables: {e}")
        finally:
            conn.close()

        tables = [{"name": r[0], "type": r[1]} for r in rows]
        return ToolResult(
            success=True,
            data={"tables": tables, "count": len(tables), "db_schema": self.db_schema},
        )

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_table_columns(self, cursor) -> list[ColumnSchema]:
        return introspect_table_via_information_schema(
            cursor,
            self.table,
            _normalize_type,
            db_schema=self.db_schema,
        )

    def _get_query_columns(self, cursor) -> list[ColumnSchema]:
        # Step 1: execute query and collect sample rows
        desc, rows = sample_query(cursor, self.query, self.sample_size)

        # Step 2: batch-resolve PostgreSQL OIDs → type names
        oids = list({col.type_code for col in desc})
        cursor.execute(
            "SELECT oid, typname FROM pg_catalog.pg_type WHERE oid = ANY(%s)",
            (oids,),
        )
        oid_to_typname = {row[0]: row[1] for row in cursor.fetchall()}

        # Step 3: build type_code → ColumnType map
        type_code_map = {
            col.type_code: _normalize_type(oid_to_typname.get(col.type_code, ""))
            for col in desc
        }

        return build_columns_from_query_result(desc, rows, type_code_map)


# ── Module-level wrappers ─────────────────────────────────────────────────────

def get_postgres_schema(
    dsn: str,
    schema_name: str,
    *,
    table: Optional[str] = None,
    query: Optional[str] = None,
    db_schema: str = "public",
) -> ToolResult:
    """Convenience wrapper: create a PostgresReader and call get_schema()."""
    return PostgresReader(
        dsn, schema_name, table=table, query=query, db_schema=db_schema
    ).get_schema()


def list_postgres_tables(dsn: str, db_schema: str = "public") -> ToolResult:
    """Convenience wrapper: list all tables and views in *db_schema*."""
    return PostgresReader(dsn, "", db_schema=db_schema).list_tables()
