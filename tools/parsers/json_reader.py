"""
JSON schema reader.

Parses a TableSchema from a raw dict — useful when the schema arrives
as a JSON payload from an API, Kafka message, or file-based source.

This is a document deserializer, not a live source introspector,
so it does not implement the SchemaReader ABC.
"""

from datetime import datetime

from ..models import ColumnSchema, TableSchema, ToolResult, ColumnType


def get_schema_from_json(schema_dict: dict) -> ToolResult:
    """
    Constructs a TableSchema from a raw dict.

    Expected format:
    {
        "table_name": "orders",
        "columns": [
            {"name": "id", "col_type": "integer", "nullable": false},
            ...
        ]
    }
    """
    try:
        columns = []
        for col in schema_dict.get("columns", []):
            # Normalise the key name so both "col_type" and "type" work
            if "type" in col and "col_type" not in col:
                col = {**col, "col_type": col["type"]}
            columns.append(ColumnSchema.from_dict(col))

        schema = TableSchema(
            table_name=schema_dict["table_name"],
            columns=columns,
            partition_keys=schema_dict.get("partition_keys", []),
            clustering_keys=schema_dict.get("clustering_keys", []),
            source=schema_dict.get("source", "json_payload"),
            captured_at=datetime.utcnow().isoformat(),
        )

        return ToolResult(
            success=True,
            data={"schema": schema.to_dict()}
        )

    except KeyError as e:
        return ToolResult(success=False, error=f"Missing required field in schema dict: {e}")
    except Exception as e:
        return ToolResult(success=False, error=f"Failed to parse schema from JSON: {e}")
