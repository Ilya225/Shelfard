"""
Shelfard — schema drift detection CLI.

Usage:
  shelfard rest snapshot <url> --name NAME [--bearer TOKEN] [--header KEY=VALUE ...]
  shelfard rest check    <url> --name NAME [--bearer TOKEN] [--header KEY=VALUE ...]
  shelfard show          <table>
  shelfard list          {schemas,subscriptions}
  shelfard subscribe     <table> --consumer NAME [--columns COL1,COL2,...]
  shelfard agent
  shelfard --help
"""

import argparse
import sys

from shelfard import (
    ColumnSchema, RestEndpointReader, TableSchema,
    compare_schemas_from_dicts, get_registered_schema, register_schema,
    get_all_schemas, get_all_consumers, subscribe_consumer,
)
from shelfard.models import ChangeSeverity


# ── ANSI colours ──────────────────────────────────────────────────────────────

_USE_COLOR = sys.stdout.isatty()


def _colour(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def red(t: str)    -> str: return _colour(t, "31")
def yellow(t: str) -> str: return _colour(t, "33")
def green(t: str)  -> str: return _colour(t, "32")
def bold(t: str)   -> str: return _colour(t, "1")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_headers(header_list: list[str]) -> dict[str, str]:
    """Parse ['Key=Value', ...] into a dict, warning on malformed entries."""
    headers: dict[str, str] = {}
    for item in header_list or []:
        if "=" not in item:
            print(f"Warning: ignoring malformed --header {item!r} (expected KEY=VALUE)")
            continue
        k, _, v = item.partition("=")
        headers[k.strip()] = v.strip()
    return headers


def _schema_from_result(result) -> TableSchema:
    """Reconstruct a TableSchema from a ToolResult's data dict."""
    s = result.data["schema"]
    return TableSchema(
        table_name=s["table_name"],
        columns=[ColumnSchema.from_dict(c) for c in s["columns"]],
        partition_keys=s.get("partition_keys", []),
        clustering_keys=s.get("clustering_keys", []),
        source=s.get("source", "unknown"),
        captured_at=s.get("captured_at"),
    )


def _print_diff(schema_name: str, diff: dict, baseline_version: str) -> None:
    if not diff["changes"]:
        print(green(f"✓ No drift detected for '{schema_name}'") +
              f"  (last snapshot: {baseline_version})")
        return

    severity = diff["overall_severity"]
    icon = "✗" if severity == ChangeSeverity.BREAKING else "⚠"
    header_fn = red if severity == ChangeSeverity.BREAKING else yellow

    print(header_fn(f"{icon} Schema drift detected for '{schema_name}'"))
    print(f"  {diff['summary']}")
    print()

    for change in diff["changes"]:
        sev = change["severity"]
        label = f"[{sev:<8}]"
        if sev == ChangeSeverity.BREAKING:
            label = red(label)
        elif sev == ChangeSeverity.WARNING:
            label = yellow(label)
        else:
            label = green(label)

        print(f"  {label} {change['change_type']:<25} '{change['column_name']}'")
        print(f"             {change['reasoning']}")
        print()


def _print_schema(schema_dict: dict, indent: int = 0) -> None:
    """Print columns recursively, indenting nested STRUCT fields."""
    pad = "  " * indent
    for col in schema_dict["columns"]:
        prefix = "." if indent > 0 else ""
        name = f"{pad}{prefix}{col['name']}"
        col_type = col["col_type"].upper()
        nullable = "nullable" if col["nullable"] else "NOT NULL"
        notes = []
        if col.get("max_length"):
            notes.append(f"max {col['max_length']}")
        if col.get("precision"):
            notes.append(f"precision {col['precision']},{col.get('scale', 0)}")
        note_str = f"   ({', '.join(notes)})" if notes else ""
        print(f"  {name:<24} {col_type:<12} {nullable}{note_str}")
        if col.get("fields"):
            _print_schema({"columns": col["fields"]}, indent + 1)


# ── REST commands ─────────────────────────────────────────────────────────────

def cmd_rest_snapshot(args) -> int:
    schema_name = args.name
    headers = _parse_headers(args.header)

    print(f"Fetching {args.url} …")
    result = RestEndpointReader(
        args.url, schema_name,
        bearer_token=args.bearer or None,
        headers=headers or None,
    ).get_schema()

    if not result.success:
        print(red(f"✗ Failed to fetch schema: {result.error}"))
        return 2

    schema = _schema_from_result(result)
    reg = register_schema(schema_name, schema)
    if not reg.success:
        print(red(f"✗ Failed to register schema: {reg.error}"))
        return 2

    version = reg.data["version_count"]
    col_count = len(schema.columns)
    print(green(f"✓ Snapshot saved: '{schema_name}' (version {version}, {col_count} top-level columns)"))
    return 0


def cmd_rest_check(args) -> int:
    schema_name = args.name
    headers = _parse_headers(args.header)

    print(f"Fetching {args.url} …")
    new_result = RestEndpointReader(
        args.url, schema_name,
        bearer_token=args.bearer or None,
        headers=headers or None,
    ).get_schema()

    if not new_result.success:
        print(red(f"✗ Failed to fetch schema: {new_result.error}"))
        return 2

    baseline_result = get_registered_schema(schema_name)
    if not baseline_result.success:
        print(red(f"✗ No snapshot found for '{schema_name}'."))
        print(f"  Run:  shelfard rest snapshot {args.url} --name {args.name}")
        return 2

    diff_result = compare_schemas_from_dicts(
        baseline_result.data["schema"],
        new_result.data["schema"],
    )
    if not diff_result.success:
        print(red(f"✗ Comparison failed: {diff_result.error}"))
        return 2

    diff = diff_result.data["diff"]
    _print_diff(schema_name, diff, baseline_result.data["version"])
    return 0 if not diff["changes"] else 1


# ── show command ──────────────────────────────────────────────────────────────

def cmd_show(args) -> int:
    result = get_registered_schema(args.table)
    if not result.success:
        print(red(f"✗ {result.error}"))
        return 2

    schema = result.data["schema"]
    version = result.data["version"]
    col_count = len(schema["columns"])
    source = schema.get("source", "unknown")

    print()
    print(bold(f"Schema: {args.table}") +
          f"  (source: {source} · version: {version} · {col_count} columns)")
    print()
    _print_schema(schema)
    print()
    return 0


# ── list command ──────────────────────────────────────────────────────────────

def cmd_list(args) -> int:
    if args.what == "schemas":
        result = get_all_schemas()
        if not result.success:
            print(red(f"✗ {result.error}"))
            return 2

        schemas = result.data["schemas"]
        if not schemas:
            print("No schemas registered yet.")
            return 0

        print(f"\nSchemas ({len(schemas)}):\n")
        for s in schemas:
            ver_word = "version" if s["version_count"] == 1 else "versions"
            print(
                f"  {s['name']:<20} {s['column_count']} cols   "
                f"{s['version_count']} {ver_word:<10} "
                f"{s.get('source') or 'unknown':<14} {s['latest_version']}"
            )
        print()

    else:  # subscriptions
        result = get_all_consumers()
        if not result.success:
            print(red(f"✗ {result.error}"))
            return 2

        consumers = result.data["consumers"]
        if not consumers:
            print("No subscriptions registered yet.")
            return 0

        print(f"\nConsumer subscriptions ({len(consumers)}):\n")
        for c in consumers:
            cols = c.get("subscribed_columns")
            cols_str = ", ".join(cols) if cols else "all columns"
            print(
                f"  {c['consumer']:<20} {c['source_table']:<14} "
                f"{cols_str:<30} {c['subscribed_at']}"
            )
        print()

    return 0


# ── subscribe command ─────────────────────────────────────────────────────────

def cmd_subscribe(args) -> int:
    columns = None
    if args.columns:
        columns = [c.strip() for c in args.columns.split(",") if c.strip()]

    result = subscribe_consumer(args.consumer, args.table, columns)
    if not result.success:
        print(red(f"✗ {result.error}"))
        return 2

    col_count = result.data["column_count"]
    if columns:
        cols_display = ", ".join(columns)
        print(green(f"✓ Subscribed '{args.consumer}' to '{args.table}' — {col_count} columns: {cols_display}"))
    else:
        print(green(f"✓ Subscribed '{args.consumer}' to '{args.table}' — all {col_count} columns captured."))

    if result.next_action_hint:
        print(f"  Note: {result.next_action_hint}")

    return 0


# ── Agent command ─────────────────────────────────────────────────────────────

def cmd_agent(args) -> int:
    from shelfard.agent import run_agent
    run_agent(model=args.model)
    return 0


# ── Parser ────────────────────────────────────────────────────────────────────

def _add_rest_subcommands(top) -> None:
    rest_p = top.add_parser("rest", help="REST API endpoint reader")
    rest_cmds = rest_p.add_subparsers(dest="command", required=True, metavar="COMMAND")

    # Shared options for all REST commands
    rest_base = argparse.ArgumentParser(add_help=False)
    rest_base.add_argument("url", help="Endpoint URL to fetch")
    rest_base.add_argument(
        "--name", metavar="NAME", required=True,
        help="Schema name used to store and retrieve from the registry",
    )
    rest_base.add_argument(
        "--bearer", metavar="TOKEN",
        help="Send Authorization: Bearer <TOKEN> with the request",
    )
    rest_base.add_argument(
        "--header", metavar="KEY=VALUE", action="append", default=[],
        help="Extra request header; can be repeated: --header X-Api-Key=abc",
    )

    snap = rest_cmds.add_parser(
        "snapshot", parents=[rest_base],
        help="Fetch the endpoint and save its schema to the registry",
    )
    snap.set_defaults(func=cmd_rest_snapshot)

    chk = rest_cmds.add_parser(
        "check", parents=[rest_base],
        help="Fetch the endpoint and compare its schema against the saved snapshot",
    )
    chk.set_defaults(func=cmd_rest_check)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shelfard",
        description=(
            "Schema drift detection CLI.\n\n"
            "Capture the shape of a data source once, then re-check it any time\n"
            "to detect unexpected schema changes before they break pipelines."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    top = parser.add_subparsers(
        dest="command", required=True,
        metavar="COMMAND",
        help="Command to run",
    )

    _add_rest_subcommands(top)
    # Future: _add_sqlite_subcommands(top)
    # Future: _add_postgres_subcommands(top)

    agent_p = top.add_parser(
        "agent",
        help="Start an interactive schema assistant (Claude or OpenAI)",
    )
    agent_p.add_argument(
        "--model", metavar="MODEL", default=None,
        help=(
            "Model to use: provider shorthand ('anthropic', 'openai') or a specific "
            "model ID ('claude-sonnet-4-6', 'gpt-4o', 'gpt-4o-mini', ...). "
            "If omitted, auto-detected from ANTHROPIC_API_KEY or OPENAI_API_KEY."
        ),
    )
    agent_p.set_defaults(func=cmd_agent)

    show_p = top.add_parser("show", help="Display a registered schema")
    show_p.add_argument("table", help="Schema name to display")
    show_p.set_defaults(func=cmd_show)

    list_p = top.add_parser("list", help="List registered schemas or consumer subscriptions")
    list_p.add_argument(
        "what",
        choices=["schemas", "subscriptions"],
        help="What to list",
    )
    list_p.set_defaults(func=cmd_list)

    sub_p = top.add_parser("subscribe", help="Subscribe a consumer to a registered schema")
    sub_p.add_argument("table", help="Source schema name")
    sub_p.add_argument(
        "--consumer", metavar="NAME", required=True,
        help="Consumer name",
    )
    sub_p.add_argument(
        "--columns", metavar="COL1,COL2,...",
        help="Comma-separated columns to subscribe to (default: all columns)",
    )
    sub_p.set_defaults(func=cmd_subscribe)

    return parser


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
