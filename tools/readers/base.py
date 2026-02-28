"""
SchemaReader — abstract base class for live database source introspectors.

Each vendor (SQLite, Postgres, BigQuery, etc.) implements this interface.
JSON schema parsing is a separate concern (document deserialization, not
live source introspection) and does not implement this ABC.
"""

from abc import ABC, abstractmethod

from ..models import ToolResult


class SchemaReader(ABC):

    @abstractmethod
    def get_schema(self) -> ToolResult:
        """
        Introspect the source and return its normalized schema.
        The target (table name, endpoint URL, etc.) is provided at construction time.

        Returns:
            ToolResult with data={"schema": TableSchema.to_dict()} on success.
        """
        ...

    @abstractmethod
    def list_tables(self) -> ToolResult:
        """
        Return all user-visible table names in the source.

        Returns:
            ToolResult with data={"tables": [...], "count": int} on success.
        """
        ...
