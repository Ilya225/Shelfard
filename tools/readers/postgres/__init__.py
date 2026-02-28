"""
PostgreSQL schema reader.

Type map is ready. Reader implementation is pending.
"""

from ...models import ColumnType


_TYPE_MAP: dict[str, ColumnType] = {
    "smallint":                  ColumnType.INTEGER,
    "integer":                   ColumnType.INTEGER,
    "int":                       ColumnType.INTEGER,
    "int2":                      ColumnType.INTEGER,
    "int4":                      ColumnType.INTEGER,
    "bigint":                    ColumnType.BIGINT,
    "int8":                      ColumnType.BIGINT,
    "real":                      ColumnType.FLOAT,
    "float4":                    ColumnType.FLOAT,
    "double precision":          ColumnType.FLOAT,
    "float8":                    ColumnType.FLOAT,
    "numeric":                   ColumnType.DECIMAL,
    "decimal":                   ColumnType.DECIMAL,
    "varchar":                   ColumnType.VARCHAR,
    "character varying":         ColumnType.VARCHAR,
    "char":                      ColumnType.VARCHAR,
    "text":                      ColumnType.TEXT,
    "boolean":                   ColumnType.BOOLEAN,
    "bool":                      ColumnType.BOOLEAN,
    "date":                      ColumnType.DATE,
    "timestamp":                 ColumnType.TIMESTAMP,
    "timestamptz":               ColumnType.TIMESTAMP,
    "timestamp with time zone":  ColumnType.TIMESTAMP,
    "json":                      ColumnType.JSON,
    "jsonb":                     ColumnType.JSON,
    "array":                     ColumnType.ARRAY,
}


def _normalize_type(raw_type: str) -> ColumnType:
    cleaned = raw_type.lower().strip()
    if "(" in cleaned:
        cleaned = cleaned[:cleaned.index("(")].strip()
    return _TYPE_MAP.get(cleaned, ColumnType.UNKNOWN)


# TODO: implement PostgresReader(SchemaReader)
