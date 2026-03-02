"""
Shared SQL introspection utilities for DB-API 2.0 compliant databases.

These helpers are database-agnostic and can be reused by PostgreSQL, MySQL,
and any other SQL backend that supports:
  - information_schema.columns  (for table/view introspection)
  - cursor.description          (for query-mode schema inference)
  - standard LIMIT clause       (for nullability sampling)
"""

from typing import Callable

from ...models import ColumnSchema, ColumnType


def sample_query(cursor, query: str, sample_size: int = 100):
    """
    Execute *query* wrapped in a LIMIT subquery and return (description, rows).

    Uses the subquery pattern ``SELECT * FROM (...) AS _q LIMIT N`` so the
    original query is never modified.  Returns the cursor.description (column
    metadata) and all fetched rows.
    """
    wrapped = f"SELECT * FROM ({query}) AS _shelfard_q LIMIT {sample_size}"
    cursor.execute(wrapped)
    rows = cursor.fetchall()
    return cursor.description, rows


def build_columns_from_query_result(
    desc,
    rows: list,
    type_code_map: dict,
) -> list[ColumnSchema]:
    """
    Build a ColumnSchema list from cursor.description and sampled rows.

    Nullability contract:
      - If *rows* is non-empty and every value in a column is non-NULL  →  NOT NULL
      - If *rows* is empty, or any value is NULL                         →  nullable

    type_code_map: dict[type_code (int) -> ColumnType]
      Callers pre-build this by resolving the vendor-specific type codes
      (e.g. PostgreSQL OIDs) to ColumnType values.
    """
    columns = []
    for i, col_desc in enumerate(desc):
        col_type = type_code_map.get(col_desc.type_code, ColumnType.UNKNOWN)

        # Nullability inferred from sampled data
        if rows:
            is_nullable = any(row[i] is None for row in rows)
        else:
            is_nullable = True  # conservative: no data → assume nullable

        max_length = None
        if col_type == ColumnType.VARCHAR:
            # internal_size is the character length for char-family columns;
            # -1 means unlimited (e.g. text), 0 means unknown
            size = col_desc.internal_size
            if size and size > 0:
                max_length = size

        precision = col_desc.precision if col_type == ColumnType.DECIMAL else None
        scale = (
            int(col_desc.scale)
            if col_type == ColumnType.DECIMAL and col_desc.scale is not None
            else None
        )

        columns.append(ColumnSchema(
            name=col_desc.name,
            col_type=col_type,
            nullable=is_nullable,
            max_length=max_length,
            precision=precision,
            scale=scale,
        ))
    return columns


def introspect_table_via_information_schema(
    cursor,
    table_name: str,
    normalize_type: Callable[[str], ColumnType],
    *,
    db_schema: str = "public",
) -> list[ColumnSchema]:
    """
    Introspect a table or view using information_schema.columns.

    Compatible with PostgreSQL and MySQL — both expose the same
    information_schema interface.  Returns columns in ordinal_position order.

    Args:
        cursor:         DB-API 2.0 cursor
        table_name:     table or view name
        normalize_type: vendor-specific fn(raw_type_str) -> ColumnType
        db_schema:      database schema name (default "public" for PostgreSQL)
    """
    cursor.execute(
        """
        SELECT column_name, data_type, is_nullable,
               character_maximum_length, numeric_precision, numeric_scale,
               column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (db_schema, table_name),
    )
    rows = cursor.fetchall()

    columns = []
    for (col_name, raw_type, is_nullable_str,
         char_max_len, num_precision, num_scale, col_default) in rows:

        col_type = normalize_type(raw_type)
        nullable = is_nullable_str.upper() == "YES"

        max_length = char_max_len if col_type == ColumnType.VARCHAR else None
        precision = (
            int(num_precision)
            if col_type == ColumnType.DECIMAL and num_precision is not None
            else None
        )
        scale = (
            int(num_scale)
            if col_type == ColumnType.DECIMAL and num_scale is not None
            else None
        )

        columns.append(ColumnSchema(
            name=col_name,
            col_type=col_type,
            nullable=nullable,
            max_length=max_length,
            precision=precision,
            scale=scale,
            default_value=str(col_default) if col_default is not None else None,
        ))
    return columns
