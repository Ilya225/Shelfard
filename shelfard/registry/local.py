"""
LocalFileRegistry — file-based SchemaRegistry implementation.

Storage layout:
    <registry_dir>/
    ├── sources/
    │   └── <table_name>.json          # versioned source schema history
    └── consumers/
        └── <consumer_name>/
            └── <table_name>.json      # versioned consumer subscription history

Each JSON file stores: {"table_name": "...", "versions": [...]}
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..models import (
    ColumnSchema, TableSchema, ConsumerSubscription, SchemaDiff, ToolResult,
    RestCheckerConfig, PostgresCheckerConfig,
)
from .base import SchemaRegistry


class LocalFileRegistry(SchemaRegistry):
    """
    File-based registry. Default root is <project_root>/schemas/.

    Pass registry_dir to override (useful in tests to isolate state).
    """

    def __init__(self, registry_dir: Optional[Path | str] = None) -> None:
        if registry_dir is None:
            registry_dir = Path(__file__).parent.parent.parent / "schemas"
        self._root = Path(registry_dir)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _sources_dir(self) -> Path:
        return self._root / "sources"

    def _consumers_dir(self) -> Path:
        return self._root / "consumers"

    def _source_path(self, table_name: str) -> Path:
        return self._sources_dir() / f"{table_name}.json"

    def _consumer_path(self, consumer_name: str, table_name: str) -> Path:
        return self._consumers_dir() / consumer_name / f"{table_name}.json"

    @staticmethod
    def _load_json(path: Path) -> dict:
        with open(path) as f:
            return json.load(f)

    @staticmethod
    def _save_json(path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    # ── Source schemas ────────────────────────────────────────────────────────

    def register_schema(self, table_name: str, schema: TableSchema) -> ToolResult:
        """Save a new version of a source schema."""
        path = self._source_path(table_name)
        try:
            if path.exists():
                registry_data = self._load_json(path)
            else:
                registry_data = {"table_name": table_name, "versions": []}

            schema.captured_at = datetime.utcnow().isoformat()
            registry_data["versions"].append(schema.to_dict())
            self._save_json(path, registry_data)

            return ToolResult(
                success=True,
                data={
                    "registered_at": schema.captured_at,
                    "version_count": len(registry_data["versions"]),
                },
                next_action_hint="Schema registered. Future drift checks will compare against this version.",
            )
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to register schema: {e}")

    def get_registered_schema(self, table_name: str, version: str = "latest") -> ToolResult:
        """Retrieve a source schema version from the registry."""
        path = self._source_path(table_name)

        if not path.exists():
            return ToolResult(
                success=False,
                error=f"No registered schema found for '{table_name}'. "
                      f"This may be a new table — consider registering it.",
                next_action_hint="If this is a new table, call register_schema() to baseline it.",
            )

        try:
            registry_data = self._load_json(path)

            if version == "latest":
                entry = registry_data["versions"][-1]
            else:
                matches = [v for v in registry_data["versions"] if v["captured_at"] == version]
                if not matches:
                    return ToolResult(
                        success=False,
                        error=f"Version '{version}' not found for table '{table_name}'.",
                    )
                entry = matches[0]

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
                next_action_hint="Use compare_schemas() to diff this against an incoming schema.",
            )
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to read registry: {e}")

    def get_all_schemas(self) -> ToolResult:
        """List all source schemas with summary metadata."""
        sources_dir = self._sources_dir()
        if not sources_dir.exists():
            return ToolResult(success=True, data={"schemas": []})

        schemas = []
        for path in sorted(sources_dir.glob("*.json")):
            try:
                data = self._load_json(path)
                versions = data.get("versions", [])
                latest = versions[-1] if versions else None
                schemas.append({
                    "name": data.get("table_name", path.stem),
                    "version_count": len(versions),
                    "latest_version": latest["captured_at"] if latest else None,
                    "source": latest.get("source", "unknown") if latest else None,
                    "column_count": len(latest.get("columns", [])) if latest else 0,
                })
            except Exception:
                pass

        return ToolResult(success=True, data={"schemas": schemas})

    # ── Consumer subscriptions ────────────────────────────────────────────────

    def subscribe_consumer(
        self,
        consumer_name: str,
        table_name: str,
        columns: Optional[list[str]] = None,
    ) -> ToolResult:
        """
        Register a consumer's dependency on a source schema.

        columns=None  → full subscription (snapshot all columns).
        columns=[...] → projection (only those columns are captured).
        """
        source_result = self.get_registered_schema(table_name)
        if not source_result.success:
            return ToolResult(
                success=False,
                error=f"Cannot subscribe to '{table_name}': source schema not registered. "
                      f"Register the source schema first.",
                next_action_hint=f"Call register_schema('{table_name}', ...) to baseline the source.",
            )

        source_version = source_result.data["version"]
        source_schema_dict = source_result.data["schema"]
        all_columns = [ColumnSchema.from_dict(c) for c in source_schema_dict["columns"]]

        hint = None
        if columns is not None:
            col_map = {c.name: c for c in all_columns}
            unknown = [c for c in columns if c not in col_map]
            selected = [col_map[c] for c in columns if c in col_map]

            if unknown:
                hint = f"Unknown columns ignored: {unknown}. Available: {list(col_map.keys())}"
            if not selected:
                return ToolResult(
                    success=False,
                    error=f"None of the requested columns {columns} exist in '{table_name}'.",
                )
            snapshot_columns = selected
        else:
            snapshot_columns = all_columns

        snapshot = TableSchema(
            table_name=table_name,
            columns=snapshot_columns,
            partition_keys=source_schema_dict.get("partition_keys", []),
            clustering_keys=source_schema_dict.get("clustering_keys", []),
            source="consumer_subscription",
            captured_at=None,
        )

        sub = ConsumerSubscription(
            consumer_name=consumer_name,
            source_table=table_name,
            subscribed_columns=columns,
            schema=snapshot,
            subscribed_at=datetime.utcnow().isoformat(),
            source_schema_version=source_version,
        )

        path = self._consumer_path(consumer_name, table_name)
        try:
            if path.exists():
                file_data = self._load_json(path)
            else:
                file_data = {"consumer_name": consumer_name, "source_table": table_name, "versions": []}

            file_data["versions"].append(sub.to_dict())
            self._save_json(path, file_data)

            return ToolResult(
                success=True,
                data={
                    "subscribed_at": sub.subscribed_at,
                    "source_schema_version": source_version,
                    "column_count": len(snapshot_columns),
                    "subscribed_columns": columns,
                },
                next_action_hint=hint,
            )
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to save subscription: {e}")

    def get_consumer_subscription(self, consumer_name: str, table_name: str) -> ToolResult:
        """Retrieve the latest subscription snapshot for a consumer/table pair."""
        path = self._consumer_path(consumer_name, table_name)

        if not path.exists():
            return ToolResult(
                success=False,
                error=f"No subscription found for consumer '{consumer_name}' on '{table_name}'.",
                next_action_hint=f"Call subscribe_consumer('{consumer_name}', '{table_name}') to create one.",
            )

        try:
            file_data = self._load_json(path)
            entry = file_data["versions"][-1]
            sub = ConsumerSubscription.from_dict(entry)
            return ToolResult(
                success=True,
                data={"subscription": sub.to_dict()},
            )
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to read subscription: {e}")

    def get_consumers_for_table(self, table_name: str) -> ToolResult:
        """List all consumers subscribed to a given source table."""
        consumers_dir = self._consumers_dir()
        if not consumers_dir.exists():
            return ToolResult(success=True, data={"consumers": []})

        consumers = []
        for consumer_dir in sorted(consumers_dir.iterdir()):
            if not consumer_dir.is_dir():
                continue
            sub_path = consumer_dir / f"{table_name}.json"
            if not sub_path.exists():
                continue
            try:
                file_data = self._load_json(sub_path)
                latest = file_data["versions"][-1]
                consumers.append({
                    "consumer": consumer_dir.name,
                    "subscribed_columns": latest.get("subscribed_columns"),
                    "subscribed_at": latest.get("subscribed_at"),
                    "source_schema_version": latest.get("source_schema_version"),
                })
            except Exception:
                pass

        return ToolResult(success=True, data={"consumers": consumers})

    def get_all_consumers(self) -> ToolResult:
        """List every consumer subscription across all tables."""
        consumers_dir = self._consumers_dir()
        if not consumers_dir.exists():
            return ToolResult(success=True, data={"consumers": []})

        consumers = []
        for consumer_dir in sorted(consumers_dir.iterdir()):
            if not consumer_dir.is_dir():
                continue
            for sub_path in sorted(consumer_dir.glob("*.json")):
                try:
                    file_data = self._load_json(sub_path)
                    latest = file_data["versions"][-1]
                    consumers.append({
                        "consumer": consumer_dir.name,
                        "source_table": file_data.get("source_table", sub_path.stem),
                        "subscribed_columns": latest.get("subscribed_columns"),
                        "subscribed_at": latest.get("subscribed_at"),
                    })
                except Exception:
                    pass

        return ToolResult(success=True, data={"consumers": consumers})

    # ── Checkers ──────────────────────────────────────────────────────────────

    def _checkers_dir(self) -> Path:
        return self._root / "checkers"

    def _checker_path(self, schema_name: str) -> Path:
        return self._checkers_dir() / f"{schema_name}.json"

    def register_checker(
        self,
        schema_name: str,
        config: RestCheckerConfig | PostgresCheckerConfig,
    ) -> ToolResult:
        """Store a checker config. Always overwrites — checkers are not versioned."""
        config.registered_at = datetime.utcnow().isoformat()
        path = self._checker_path(schema_name)
        try:
            self._save_json(path, config.to_dict())
            return ToolResult(
                success=True,
                data={"schema_name": schema_name, "registered_at": config.registered_at},
            )
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to register checker: {e}")

    def get_checker(self, schema_name: str) -> ToolResult:
        """Retrieve the checker config for the given schema."""
        path = self._checker_path(schema_name)
        if not path.exists():
            return ToolResult(
                success=False,
                error=f"No checker registered for '{schema_name}'.",
            )
        try:
            raw = self._load_json(path)
            return ToolResult(success=True, data={"checker": raw})
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to read checker: {e}")

    def get_all_checkers(self) -> ToolResult:
        """List all registered checkers with summary metadata."""
        checkers_dir = self._checkers_dir()
        if not checkers_dir.exists():
            return ToolResult(success=True, data={"checkers": []})
        result = []
        for f in sorted(checkers_dir.glob("*.json")):
            try:
                c = self._load_json(f)
                result.append({
                    "schema_name": c["schema_name"],
                    "checker_type": c.get("checker_type", "rest"),
                    "url": c.get("url", ""),       # rest checkers
                    "dsn": c.get("dsn", ""),       # postgres checkers
                    "env": c.get("env", []),
                    "registered_at": c.get("registered_at"),
                })
            except Exception:
                pass
        return ToolResult(success=True, data={"checkers": result})

    def run_checker(self, schema_name: str) -> ToolResult:
        """Load the checker config and run the live drift check."""
        result = self.get_checker(schema_name)
        if not result.success:
            return result

        config_dict = result.data["checker"]
        checker_type = config_dict.get("checker_type", "rest")

        if checker_type == "rest":
            from ..tools.rest import RestChecker
            config = RestCheckerConfig.from_dict(config_dict)
            return RestChecker(config, self).run()
        elif checker_type == "postgres":
            from ..tools.postgres import PostgresChecker
            config = PostgresCheckerConfig.from_dict(config_dict)
            return PostgresChecker(config, self).run()
        else:
            return ToolResult(
                success=False,
                error=f"Unknown checker type: {checker_type!r}",
            )

    # ── Impact analysis ───────────────────────────────────────────────────────

    def get_consumers_affected_by_diff(self, table_name: str, diff: SchemaDiff) -> ToolResult:
        """
        Given a SchemaDiff on a source table, return which consumers are impacted.

        A consumer with a full subscription is affected by any change.
        A consumer with a projection is affected only if one of their columns changed.
        Column names from nested STRUCTs use dot-notation (e.g. "address.zip");
        the top-level name ("address") is checked against the subscription.
        """
        consumers_result = self.get_consumers_for_table(table_name)
        if not consumers_result.success:
            return consumers_result

        if not diff.has_changes:
            return ToolResult(success=True, data={"affected": []})

        # Collect top-level changed column names (strip nested qualifiers)
        changed_cols = {c.column_name.split(".")[0] for c in diff.changes}

        affected = []
        for entry in consumers_result.data["consumers"]:
            subscribed = entry["subscribed_columns"]
            if subscribed is None:
                # Full subscription — any change is impactful
                impacted = [c.to_dict() for c in diff.changes]
            else:
                subscribed_set = set(subscribed)
                impacted = [
                    c.to_dict() for c in diff.changes
                    if c.column_name.split(".")[0] in subscribed_set
                ]

            if impacted:
                affected.append({
                    "consumer": entry["consumer"],
                    "subscribed_columns": subscribed,
                    "impacted_changes": impacted,
                })

        return ToolResult(
            success=True,
            data={"affected": affected},
            next_action_hint=(
                f"{len(affected)} consumer(s) affected by changes to '{table_name}'."
                if affected else None
            ),
        )
