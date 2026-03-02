"""
BigQuery schema reader.

Type map is ready. Reader implementation is pending.
"""

from ...models import ColumnType


_TYPE_MAP: dict[str, ColumnType] = {
    "int64":      ColumnType.BIGINT,
    "integer":    ColumnType.INTEGER,
    "float64":    ColumnType.FLOAT,
    "float":      ColumnType.FLOAT,
    "numeric":    ColumnType.DECIMAL,
    "bignumeric": ColumnType.DECIMAL,
    "bool":       ColumnType.BOOLEAN,
    "boolean":    ColumnType.BOOLEAN,
    "string":     ColumnType.VARCHAR,
    "bytes":      ColumnType.UNKNOWN,
    "date":       ColumnType.DATE,
    "datetime":   ColumnType.TIMESTAMP,
    "timestamp":  ColumnType.TIMESTAMP,
    "json":       ColumnType.JSON,
    "array":      ColumnType.ARRAY,
    "struct":     ColumnType.JSON,
}


def _normalize_type(raw_type: str) -> ColumnType:
    cleaned = raw_type.lower().strip()
    if "(" in cleaned:
        cleaned = cleaned[:cleaned.index("(")].strip()
    return _TYPE_MAP.get(cleaned, ColumnType.UNKNOWN)


# TODO: implement BigQueryReader(SchemaReader)
