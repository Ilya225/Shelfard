"""
Core data models for the schema drift agent.
Everything is typed, serializable, and carries enough context
for the LLM to reason about it without needing to call more tools.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional
import json


# ─────────────────────────────────────────────
# Schema primitives
# ─────────────────────────────────────────────

class ColumnType(str, Enum):
    """Normalized type system — maps vendor types to these buckets."""
    INTEGER   = "integer"
    BIGINT    = "bigint"
    FLOAT     = "float"
    DECIMAL   = "decimal"
    VARCHAR   = "varchar"
    TEXT      = "text"
    BOOLEAN   = "boolean"
    DATE      = "date"
    TIMESTAMP = "timestamp"
    JSON      = "json"
    ARRAY     = "array"
    STRUCT    = "struct"
    UNKNOWN   = "unknown"


@dataclass
class ColumnSchema:
    name: str
    col_type: ColumnType
    nullable: bool = True
    max_length: Optional[int] = None       # for varchar
    precision: Optional[int] = None        # for decimal
    scale: Optional[int] = None            # for decimal
    default_value: Optional[str] = None
    description: Optional[str] = None
    fields: Optional[list[ColumnSchema]] = field(default=None)  # sub-fields for STRUCT columns

    def to_dict(self) -> dict:
        return asdict(self)

    def __eq__(self, other):
        if not isinstance(other, ColumnSchema):
            return False
        return (
            self.col_type == other.col_type and
            self.nullable == other.nullable and
            self.max_length == other.max_length and
            self.precision == other.precision and
            self.scale == other.scale and
            self.fields == other.fields
        )

    @classmethod
    def from_dict(cls, col: dict) -> ColumnSchema:
        """Recursively reconstruct a ColumnSchema (and its nested fields) from a dict."""
        col_type_str = col.get("col_type", "unknown")
        try:
            col_type = ColumnType(col_type_str.lower())
        except ValueError:
            col_type = ColumnType.UNKNOWN

        nested = None
        if col_type == ColumnType.STRUCT and col.get("fields"):
            nested = [cls.from_dict(f) for f in col["fields"]]

        return cls(
            name=col["name"],
            col_type=col_type,
            nullable=col.get("nullable", True),
            max_length=col.get("max_length"),
            precision=col.get("precision"),
            scale=col.get("scale"),
            default_value=col.get("default_value"),
            description=col.get("description"),
            fields=nested,
        )


@dataclass
class TableSchema:
    table_name: str
    columns: list[ColumnSchema]
    partition_keys: list[str] = field(default_factory=list)
    clustering_keys: list[str] = field(default_factory=list)
    source: str = "unknown"          # "sqlite", "snowflake", "bigquery", etc.
    captured_at: Optional[str] = None

    def column_map(self) -> dict[str, ColumnSchema]:
        return {col.name: col for col in self.columns}

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)


# ─────────────────────────────────────────────
# Diff models
# ─────────────────────────────────────────────

class ChangeSeverity(str, Enum):
    SAFE     = "SAFE"      # additive, non-breaking
    WARNING  = "WARNING"   # potentially breaking, needs review
    BREAKING = "BREAKING"  # will break downstream consumers


class ChangeType(str, Enum):
    COLUMN_ADDED        = "column_added"
    COLUMN_REMOVED      = "column_removed"
    TYPE_WIDENED        = "type_widened"       # e.g. int → bigint, varchar(50) → varchar(200)
    TYPE_CHANGED        = "type_changed"       # e.g. int → varchar — almost always breaking
    NULLABILITY_RELAXED = "nullability_relaxed" # NOT NULL → NULL — usually safe
    NULLABILITY_TIGHTENED = "nullability_tightened" # NULL → NOT NULL — breaking if data exists
    DEFAULT_CHANGED     = "default_changed"
    COLUMN_REORDERED    = "column_reordered"   # safe for named access, breaking for positional


@dataclass
class ColumnChange:
    change_type: ChangeType
    column_name: str
    old_value: Optional[dict] = None    # serialized ColumnSchema or specific field
    new_value: Optional[dict] = None
    severity: ChangeSeverity = ChangeSeverity.SAFE
    reasoning: str = ""                 # human-readable explanation of severity decision

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SchemaDiff:
    table_name: str
    old_schema_version: Optional[str]
    new_schema_version: Optional[str]
    changes: list[ColumnChange]
    overall_severity: ChangeSeverity = ChangeSeverity.SAFE
    summary: str = ""

    @property
    def has_changes(self) -> bool:
        return len(self.changes) > 0

    @property
    def breaking_changes(self) -> list[ColumnChange]:
        return [c for c in self.changes if c.severity == ChangeSeverity.BREAKING]

    @property
    def safe_changes(self) -> list[ColumnChange]:
        return [c for c in self.changes if c.severity == ChangeSeverity.SAFE]

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)


# ─────────────────────────────────────────────
# Tool result wrapper
# ─────────────────────────────────────────────

@dataclass
class ToolResult:
    """
    Wrapper returned by every tool. The agent sees this — it needs to be
    self-describing enough that the LLM knows what happened and what to do next.
    """
    success: bool
    data: Optional[dict] = None
    error: Optional[str] = None
    next_action_hint: Optional[str] = None   # optional nudge for the agent

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)
