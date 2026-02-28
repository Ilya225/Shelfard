"""
Schema Registry

Stores and retrieves versioned TableSchema snapshots.
File-based for Day 1 — replace with Glue Data Catalog,
Confluent Schema Registry, etc. in later phases.
"""

import json
from datetime import datetime
from pathlib import Path

from .models import ColumnSchema, TableSchema, ToolResult


REGISTRY_DIR = Path(__file__).parent.parent / "schemas"


def _registry_path(table_name: str) -> Path:
    return REGISTRY_DIR / f"{table_name}.json"


def get_registered_schema(table_name: str, version: str = "latest") -> ToolResult:
    """
    Reads the previously registered (known-good) schema from the registry.
    This is the baseline we compare incoming schemas against.

    Args:
        table_name: Name of the table
        version: "latest" or a specific ISO timestamp string

    Returns:
        ToolResult with TableSchema dict in data, or error if not found
    """
    path = _registry_path(table_name)

    if not path.exists():
        return ToolResult(
            success=False,
            error=f"No registered schema found for '{table_name}'. "
                  f"This may be a new table — consider registering it.",
            next_action_hint="If this is a new table, call register_schema() to baseline it."
        )

    try:
        with open(path) as f:
            registry_data = json.load(f)

        # Registry stores list of versions; "latest" picks the last one
        if version == "latest":
            entry = registry_data["versions"][-1]
        else:
            matches = [v for v in registry_data["versions"] if v["captured_at"] == version]
            if not matches:
                return ToolResult(
                    success=False,
                    error=f"Version '{version}' not found for table '{table_name}'."
                )
            entry = matches[0]

        # Reconstruct ColumnSchema objects (recursively handles STRUCT fields)
        columns = [ColumnSchema.from_dict(col) for col in entry["columns"]]

        schema = TableSchema(
            table_name=table_name,
            columns=columns,
            partition_keys=entry.get("partition_keys", []),
            clustering_keys=entry.get("clustering_keys", []),
            source=entry.get("source", "registry"),
            captured_at=entry["captured_at"],
        )

        return ToolResult(
            success=True,
            data={"schema": schema.to_dict(), "version": entry["captured_at"]},
            next_action_hint="Use compare_schemas() to diff this against an incoming schema."
        )

    except Exception as e:
        return ToolResult(success=False, error=f"Failed to read registry: {e}")


def register_schema(table_name: str, schema: TableSchema) -> ToolResult:
    """
    Saves a schema to the registry as a new version.
    Called after successful remediation, or to baseline a new table.
    """
    REGISTRY_DIR.mkdir(exist_ok=True)
    path = _registry_path(table_name)

    try:
        if path.exists():
            with open(path) as f:
                registry_data = json.load(f)
        else:
            registry_data = {"table_name": table_name, "versions": []}

        schema.captured_at = datetime.utcnow().isoformat()
        registry_data["versions"].append(schema.to_dict())

        with open(path, "w") as f:
            json.dump(registry_data, f, indent=2, default=str)

        return ToolResult(
            success=True,
            data={"registered_at": schema.captured_at, "version_count": len(registry_data["versions"])},
            next_action_hint="Schema registered. Future drift checks will compare against this version."
        )

    except Exception as e:
        return ToolResult(success=False, error=f"Failed to register schema: {e}")
