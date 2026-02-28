"""
Vendor-agnostic type utilities.

Vendor-specific type maps live in each reader file (readers/sqlite.py, etc.).
This module contains only logic that operates on the canonical ColumnType enum,
independent of any vendor's raw type strings.
"""

from .models import ColumnType


# ─────────────────────────────────────────────
# Type widening rules
# These define which type changes are "safe widening" vs breaking
# ─────────────────────────────────────────────

# (from_type, to_type) → is_safe_widening
TYPE_WIDENING_RULES: dict[tuple[ColumnType, ColumnType], bool] = {
    # Numeric widenings — generally safe
    (ColumnType.INTEGER, ColumnType.BIGINT):    True,
    (ColumnType.INTEGER, ColumnType.FLOAT):     True,   # precision loss possible, warn
    (ColumnType.INTEGER, ColumnType.DECIMAL):   True,
    (ColumnType.FLOAT,   ColumnType.DECIMAL):   True,

    # String widenings — safe if length increases
    (ColumnType.VARCHAR, ColumnType.TEXT):      True,   # always widening

    # Dangerous type changes
    (ColumnType.INTEGER,   ColumnType.VARCHAR): False,  # breaking — consumers expect numeric
    (ColumnType.FLOAT,     ColumnType.VARCHAR): False,
    (ColumnType.DECIMAL,   ColumnType.VARCHAR): False,
    (ColumnType.TIMESTAMP, ColumnType.VARCHAR): False,
    (ColumnType.BOOLEAN,   ColumnType.INTEGER): False,  # technically ok but signals schema confusion
    (ColumnType.JSON,      ColumnType.VARCHAR): False,
    (ColumnType.VARCHAR,   ColumnType.INTEGER): False,  # will fail if non-numeric strings exist
}


def is_safe_widening(from_type: ColumnType, to_type: ColumnType) -> bool:
    """
    Returns True if changing from_type to to_type is a safe widening.
    Returns False if unknown — caller should treat as WARNING.
    """
    return TYPE_WIDENING_RULES.get((from_type, to_type), False)


def extract_length(raw_type: str) -> int | None:
    """Extract length from varchar(255) → 255. Returns None if not present."""
    if "(" in raw_type and ")" in raw_type:
        try:
            inner = raw_type[raw_type.index("(") + 1 : raw_type.index(")")]
            parts = inner.split(",")
            return int(parts[0].strip())
        except (ValueError, IndexError):
            return None
    return None
