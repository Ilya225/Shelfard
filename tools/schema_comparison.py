"""
Layer 2: Schema Comparison Engine

This is the core of Day 1. Pure deterministic logic —
no LLM involved here, only in interpreting the results.

The output SchemaDiff is designed to be:
1. Complete — the agent shouldn't need to re-examine the schemas
2. Self-explaining — each change includes its own reasoning string
3. Actionable — severity is pre-classified so the agent can route appropriately
"""

from .models import (
    ColumnSchema, TableSchema, SchemaDiff, ColumnChange,
    ChangeType, ChangeSeverity, ToolResult, ColumnType
)
from .type_normalizer import is_safe_widening
from .parsers.json_reader import get_schema_from_json


# ─────────────────────────────────────────────
# Severity classification rules
# These are deterministic rules that avoid wasting LLM tokens
# on clear-cut cases. The LLM reasons about ambiguous ones.
# ─────────────────────────────────────────────

def _classify_added_column(col: ColumnSchema) -> ChangeSeverity:
    """
    Adding a column is SAFE if nullable or has a default.
    BREAKING if NOT NULL with no default — existing rows can't satisfy it.
    """
    if not col.nullable and col.default_value is None:
        return ChangeSeverity.BREAKING
    return ChangeSeverity.SAFE


def _classify_removed_column(col: ColumnSchema) -> ChangeSeverity:
    """Removing any column is always BREAKING — downstream SELECT * or named access fails."""
    return ChangeSeverity.BREAKING


def _classify_type_change(
    old_col: ColumnSchema,
    new_col: ColumnSchema
) -> tuple[ChangeType, ChangeSeverity, str]:
    """
    Returns (change_type, severity, reasoning).
    """
    old_t = old_col.col_type
    new_t = new_col.col_type

    # Same logical type — check length/precision changes
    if old_t == new_t:
        if old_t == ColumnType.VARCHAR:
            old_len = old_col.max_length or 0
            new_len = new_col.max_length or 0
            if new_len > old_len or new_len == 0:
                return (
                    ChangeType.TYPE_WIDENED,
                    ChangeSeverity.SAFE,
                    f"VARCHAR length increased from {old_len} to {new_len} — safe widening."
                )
            elif new_len < old_len:
                return (
                    ChangeType.TYPE_CHANGED,
                    ChangeSeverity.BREAKING,
                    f"VARCHAR length decreased from {old_len} to {new_len} — "
                    f"existing data may be truncated on write."
                )

        if old_t == ColumnType.DECIMAL:
            old_prec = old_col.precision or 0
            new_prec = new_col.precision or 0
            if new_prec >= old_prec:
                return (
                    ChangeType.TYPE_WIDENED,
                    ChangeSeverity.SAFE,
                    f"DECIMAL precision increased from {old_prec} to {new_prec}."
                )
            else:
                return (
                    ChangeType.TYPE_CHANGED,
                    ChangeSeverity.WARNING,
                    f"DECIMAL precision decreased from {old_prec} to {new_prec} — "
                    f"may lose precision on existing data."
                )

        # Same type, nothing else changed — shouldn't normally reach here
        return (ChangeType.TYPE_CHANGED, ChangeSeverity.SAFE, "Same type, no attribute changes.")

    # Different types — check widening rules
    if is_safe_widening(old_t, new_t):
        return (
            ChangeType.TYPE_WIDENED,
            ChangeSeverity.SAFE,
            f"{old_t.value} → {new_t.value} is a safe numeric widening."
        )

    # Unknown or dangerous type change
    return (
        ChangeType.TYPE_CHANGED,
        ChangeSeverity.BREAKING,
        f"{old_t.value} → {new_t.value} is a potentially breaking type change. "
        f"Downstream consumers expecting {old_t.value} will fail or produce incorrect results."
    )


def _classify_nullability_change(
    old_col: ColumnSchema,
    new_col: ColumnSchema
) -> tuple[ChangeSeverity, str]:
    if not old_col.nullable and new_col.nullable:
        return (
            ChangeSeverity.SAFE,
            "Column changed from NOT NULL to NULL — safe, relaxes constraints."
        )
    else:  # nullable → not nullable
        return (
            ChangeSeverity.BREAKING,
            "Column changed from NULL to NOT NULL — "
            "any existing NULL values will violate the constraint."
        )


# ─────────────────────────────────────────────
# Core diff logic (recursive, prefix-aware)
# ─────────────────────────────────────────────

def _diff_column_list(
    old_columns: list[ColumnSchema],
    new_columns: list[ColumnSchema],
    name_prefix: str = "",
) -> list[ColumnChange]:
    """
    Compare two lists of ColumnSchema and return all changes.

    name_prefix is dot-notation context for nested STRUCT fields,
    e.g. "address." produces change names like "address.street".
    Calls itself recursively when both sides have a STRUCT column.
    """
    old_cols = {col.name: col for col in old_columns}
    new_cols = {col.name: col for col in new_columns}

    changes: list[ColumnChange] = []

    # ── Detect removed columns ──────────────────────────────────────
    for col_name, old_col in old_cols.items():
        if col_name not in new_cols:
            qualified = f"{name_prefix}{col_name}"
            changes.append(ColumnChange(
                change_type=ChangeType.COLUMN_REMOVED,
                column_name=qualified,
                old_value=old_col.to_dict(),
                new_value=None,
                severity=_classify_removed_column(old_col),
                reasoning=f"Column '{qualified}' ({old_col.col_type.value}) was removed. "
                          f"All consumers reading this column will fail."
            ))

    # ── Detect added columns ────────────────────────────────────────
    for col_name, new_col in new_cols.items():
        if col_name not in old_cols:
            qualified = f"{name_prefix}{col_name}"
            severity = _classify_added_column(new_col)
            reasoning = (
                f"Column '{qualified}' ({new_col.col_type.value}) was added. "
                + (
                    "NOT NULL with no default — will break INSERT statements on existing pipeline logic."
                    if severity == ChangeSeverity.BREAKING
                    else "Nullable or has default — safe addition, existing queries unaffected."
                )
            )
            changes.append(ColumnChange(
                change_type=ChangeType.COLUMN_ADDED,
                column_name=qualified,
                old_value=None,
                new_value=new_col.to_dict(),
                severity=severity,
                reasoning=reasoning
            ))

    # ── Detect modifications on existing columns ────────────────────
    for col_name in old_cols:
        if col_name not in new_cols:
            continue  # already handled above

        old_col = old_cols[col_name]
        new_col = new_cols[col_name]
        qualified = f"{name_prefix}{col_name}"

        # Both sides are STRUCT — recurse into nested fields
        if old_col.col_type == ColumnType.STRUCT and new_col.col_type == ColumnType.STRUCT:
            changes.extend(
                _diff_column_list(
                    old_col.fields or [],
                    new_col.fields or [],
                    f"{qualified}.",
                )
            )
            continue

        # Type change (includes same-type attribute changes like precision, length)
        if (old_col.col_type != new_col.col_type
                or old_col.max_length != new_col.max_length
                or old_col.precision != new_col.precision
                or old_col.scale != new_col.scale):
            change_type, severity, reasoning = _classify_type_change(old_col, new_col)
            changes.append(ColumnChange(
                change_type=change_type,
                column_name=qualified,
                old_value={"col_type": old_col.col_type.value, "max_length": old_col.max_length},
                new_value={"col_type": new_col.col_type.value, "max_length": new_col.max_length},
                severity=severity,
                reasoning=reasoning
            ))

        # Nullability change
        elif old_col.nullable != new_col.nullable:
            severity, reasoning = _classify_nullability_change(old_col, new_col)
            change_type = (
                ChangeType.NULLABILITY_RELAXED
                if not old_col.nullable and new_col.nullable
                else ChangeType.NULLABILITY_TIGHTENED
            )
            changes.append(ColumnChange(
                change_type=change_type,
                column_name=qualified,
                old_value={"nullable": old_col.nullable},
                new_value={"nullable": new_col.nullable},
                severity=severity,
                reasoning=reasoning
            ))

        # Default value change
        elif old_col.default_value != new_col.default_value:
            changes.append(ColumnChange(
                change_type=ChangeType.DEFAULT_CHANGED,
                column_name=qualified,
                old_value={"default_value": old_col.default_value},
                new_value={"default_value": new_col.default_value},
                severity=ChangeSeverity.WARNING,
                reasoning=f"Default value changed from '{old_col.default_value}' "
                          f"to '{new_col.default_value}'. "
                          f"New rows will have different defaults — verify this is intentional."
            ))

    # ── Detect column reordering ────────────────────────────────────
    shared_cols_old = [c for c in old_columns if c.name in new_cols]
    shared_cols_new = [c for c in new_columns if c.name in old_cols]
    old_order = [c.name for c in shared_cols_old]
    new_order = [c.name for c in shared_cols_new]

    if old_order != new_order:
        reorder_name = f"{name_prefix}<multiple>" if name_prefix else "<multiple>"
        changes.append(ColumnChange(
            change_type=ChangeType.COLUMN_REORDERED,
            column_name=reorder_name,
            old_value={"order": old_order},
            new_value={"order": new_order},
            severity=ChangeSeverity.WARNING,
            reasoning="Column order changed. Named column access is unaffected, "
                      "but any positional access (SELECT *, CSV exports, legacy COBOL-style consumers) "
                      "may read wrong values silently — the most dangerous kind of bug."
        ))

    return changes


# ─────────────────────────────────────────────
# Main comparison function
# ─────────────────────────────────────────────

def compare_schemas(old_schema: TableSchema, new_schema: TableSchema) -> ToolResult:
    """
    Core diff engine. Compares two TableSchema objects and returns a SchemaDiff.

    Args:
        old_schema: The registered/baseline schema
        new_schema: The incoming/observed schema

    Returns:
        ToolResult with SchemaDiff in data
    """
    try:
        changes = _diff_column_list(old_schema.columns, new_schema.columns)

        # ── Compute overall severity ────────────────────────────────────
        severities = [c.severity for c in changes]
        if ChangeSeverity.BREAKING in severities:
            overall = ChangeSeverity.BREAKING
        elif ChangeSeverity.WARNING in severities:
            overall = ChangeSeverity.WARNING
        else:
            overall = ChangeSeverity.SAFE

        # ── Build human-readable summary ────────────────────────────────
        n_breaking = sum(1 for c in changes if c.severity == ChangeSeverity.BREAKING)
        n_warning  = sum(1 for c in changes if c.severity == ChangeSeverity.WARNING)
        n_safe     = sum(1 for c in changes if c.severity == ChangeSeverity.SAFE)

        if not changes:
            summary = "No schema changes detected. Schemas are identical."
        else:
            parts = []
            if n_breaking: parts.append(f"{n_breaking} breaking")
            if n_warning:  parts.append(f"{n_warning} warning")
            if n_safe:     parts.append(f"{n_safe} safe")
            summary = f"{len(changes)} change(s) detected: {', '.join(parts)}."

        diff = SchemaDiff(
            table_name=old_schema.table_name,
            old_schema_version=old_schema.captured_at,
            new_schema_version=new_schema.captured_at,
            changes=changes,
            overall_severity=overall,
            summary=summary,
        )

        hint = {
            ChangeSeverity.SAFE:     "All changes are safe. Consider calling register_schema() to update the baseline.",
            ChangeSeverity.WARNING:  "Some changes need review. Inspect WARNING items before proceeding.",
            ChangeSeverity.BREAKING: "BREAKING changes detected. Do NOT auto-apply. Escalate or generate a migration plan.",
        }[overall]

        return ToolResult(
            success=True,
            data={"diff": diff.to_dict()},
            next_action_hint=hint
        )

    except Exception as e:
        return ToolResult(success=False, error=f"Schema comparison failed: {e}")


def compare_schemas_from_dicts(old_dict: dict, new_dict: dict) -> ToolResult:
    """
    Convenience wrapper — takes raw dicts (as returned by schema acquisition tools)
    and compares them. This is what the agent will call most often.
    """
    old_result = get_schema_from_json(old_dict)
    new_result = get_schema_from_json(new_dict)

    if not old_result.success:
        return ToolResult(success=False, error=f"Old schema invalid: {old_result.error}")
    if not new_result.success:
        return ToolResult(success=False, error=f"New schema invalid: {new_result.error}")

    def dict_to_table_schema(d: dict) -> TableSchema:
        s = d["schema"]
        return TableSchema(
            table_name=s["table_name"],
            columns=[ColumnSchema.from_dict(c) for c in s["columns"]],
            partition_keys=s.get("partition_keys", []),
            captured_at=s.get("captured_at"),
            source=s.get("source", "unknown"),
        )

    return compare_schemas(
        dict_to_table_schema(old_result.data),
        dict_to_table_schema(new_result.data),
    )
