"""
Snowflake schema reader.

Type map is ready. Reader implementation is pending.
"""

from ...models import ColumnType


_TYPE_MAP: dict[str, ColumnType] = {
    "number":           ColumnType.DECIMAL,
    "decimal":          ColumnType.DECIMAL,
    "numeric":          ColumnType.DECIMAL,
    "int":              ColumnType.INTEGER,
    "integer":          ColumnType.INTEGER,
    "bigint":           ColumnType.BIGINT,
    "smallint":         ColumnType.INTEGER,
    "tinyint":          ColumnType.INTEGER,
    "byteint":          ColumnType.INTEGER,
    "float":            ColumnType.FLOAT,
    "float4":           ColumnType.FLOAT,
    "float8":           ColumnType.FLOAT,
    "double":           ColumnType.FLOAT,
    "double precision": ColumnType.FLOAT,
    "real":             ColumnType.FLOAT,
    "varchar":          ColumnType.VARCHAR,
    "char":             ColumnType.VARCHAR,
    "character":        ColumnType.VARCHAR,
    "string":           ColumnType.VARCHAR,
    "text":             ColumnType.TEXT,
    "boolean":          ColumnType.BOOLEAN,
    "date":             ColumnType.DATE,
    "timestamp":        ColumnType.TIMESTAMP,
    "timestamp_ltz":    ColumnType.TIMESTAMP,
    "timestamp_ntz":    ColumnType.TIMESTAMP,
    "timestamp_tz":     ColumnType.TIMESTAMP,
    "variant":          ColumnType.JSON,
    "object":           ColumnType.JSON,
    "array":            ColumnType.ARRAY,
}


def _normalize_type(raw_type: str) -> ColumnType:
    cleaned = raw_type.lower().strip()
    if "(" in cleaned:
        cleaned = cleaned[:cleaned.index("(")].strip()
    return _TYPE_MAP.get(cleaned, ColumnType.UNKNOWN)


# TODO: implement SnowflakeReader(SchemaReader)
