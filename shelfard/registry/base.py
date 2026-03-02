"""
SchemaRegistry — abstract base class.

Any backend (local filesystem, S3, GCS, SQL) must implement this interface.
All methods return ToolResult so they are usable directly as LLM agent tools.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from ..models import TableSchema, SchemaDiff, ToolResult, RestCheckerConfig, PostgresCheckerConfig


class SchemaRegistry(ABC):

    # ── Source schemas ────────────────────────────────────────────────────────

    @abstractmethod
    def register_schema(self, table_name: str, schema: TableSchema) -> ToolResult:
        """Save a new version of a source schema to the registry."""
        ...

    @abstractmethod
    def get_registered_schema(self, table_name: str, version: str = "latest") -> ToolResult:
        """
        Retrieve a registered source schema.

        version: "latest" or a specific ISO timestamp string.
        Returns ToolResult with data={"schema": {...}, "version": "..."}.
        """
        ...

    @abstractmethod
    def get_all_schemas(self) -> ToolResult:
        """List all source schemas with summary metadata."""
        ...

    # ── Consumer subscriptions ────────────────────────────────────────────────

    @abstractmethod
    def subscribe_consumer(
        self,
        consumer_name: str,
        table_name: str,
        columns: Optional[list[str]] = None,
    ) -> ToolResult:
        """
        Register a consumer's dependency on a source schema.

        columns=None  → full subscription (snapshot entire source schema).
        columns=[...] → projection (only the listed columns are captured).

        The latest registered source schema must already exist.
        Returns ToolResult with data={"subscribed_at": ..., "column_count": ...}.
        """
        ...

    @abstractmethod
    def get_consumer_subscription(self, consumer_name: str, table_name: str) -> ToolResult:
        """
        Retrieve the latest subscription snapshot for a consumer/table pair.

        Returns ToolResult with data={"subscription": ConsumerSubscription.to_dict()}.
        """
        ...

    @abstractmethod
    def get_consumers_for_table(self, table_name: str) -> ToolResult:
        """
        List all consumers that have subscribed to a given source table.

        Returns ToolResult with data={"consumers": [{"consumer": ..., "subscribed_columns": ...}]}.
        """
        ...

    @abstractmethod
    def get_all_consumers(self) -> ToolResult:
        """
        List every consumer subscription across all tables.

        Returns ToolResult with data={"consumers": [...]}.
        """
        ...

    # ── Checkers ──────────────────────────────────────────────────────────────

    @abstractmethod
    def register_checker(
        self,
        schema_name: str,
        config: RestCheckerConfig | PostgresCheckerConfig,
    ) -> ToolResult:
        """Store a checker config for the given schema."""
        ...

    @abstractmethod
    def get_checker(self, schema_name: str) -> ToolResult:
        """
        Retrieve the checker config for the given schema.

        Returns ToolResult with data={"checker": RestCheckerConfig.to_dict()}.
        """
        ...

    @abstractmethod
    def get_all_checkers(self) -> ToolResult:
        """
        List all registered checkers with summary metadata.

        Returns ToolResult with data={"checkers": [...]}.
        """
        ...

    @abstractmethod
    def run_checker(self, schema_name: str) -> ToolResult:
        """
        Load the checker config for schema_name, instantiate the appropriate
        Checker, and call run().

        Returns the same ToolResult as Checker.run().
        """
        ...

    # ── Impact analysis ───────────────────────────────────────────────────────

    @abstractmethod
    def get_consumers_affected_by_diff(self, table_name: str, diff: SchemaDiff) -> ToolResult:
        """
        Given a SchemaDiff on a source table, return which consumers are impacted.

        A consumer is affected if:
          - They have a full subscription and the diff has any changes, OR
          - Any changed column name appears in their subscribed_columns list.

        Returns ToolResult with data={"affected": [{"consumer": ..., "impacted_changes": [...]}]}.
        """
        ...
