"""
PostgreSQL drift checker.

Loads a stored :class:`~shelfard.models.PostgresCheckerConfig`, resolves
``$VAR`` placeholders in the DSN and query at run time (never at storage time),
fetches the live schema via :class:`~shelfard.tools.postgres.reader.PostgresReader`,
diffs it against the registered baseline, and returns a :class:`ToolResult`.
"""

import os

from ..base import Checker
from ...models import PostgresCheckerConfig, ToolResult


class PostgresChecker(Checker):
    """Run a stored PostgreSQL drift-check configuration."""

    def __init__(self, config: PostgresCheckerConfig, registry) -> None:
        self.config = config
        self.registry = registry

    def run(self) -> ToolResult:
        # 1. Validate that all required env vars are present
        missing = [name for name in self.config.env if name not in os.environ]
        if missing:
            return ToolResult(
                success=False,
                error=f"Missing required environment variables: {', '.join(missing)}",
                next_action_hint=f"Export the missing variables before running: {', '.join(missing)}",
            )

        # 2. Resolve $VAR placeholders in DSN and query (if any)
        resolved_dsn = _substitute(self.config.dsn, self.config.env)
        resolved_query = (
            _substitute(self.config.query, self.config.env)
            if self.config.query
            else None
        )

        # 3. Fetch live schema
        from .reader import PostgresReader

        reader = PostgresReader(
            resolved_dsn,
            self.config.schema_name,
            table=self.config.table,
            query=resolved_query,
            db_schema=self.config.db_schema,
            sample_size=self.config.sample_size,
        )
        live_result = reader.get_schema()
        if not live_result.success:
            return ToolResult(
                success=False,
                error=f"Failed to fetch live schema: {live_result.error}",
            )

        # 4. Load baseline from registry
        baseline_result = self.registry.get_registered_schema(self.config.schema_name)
        if not baseline_result.success:
            return ToolResult(
                success=False,
                error=f"No baseline found for '{self.config.schema_name}'",
                next_action_hint=(
                    "Run 'shelfard postgres snapshot' first to capture a baseline"
                ),
            )

        # 5. Diff
        from ...schema_comparison import compare_schemas_from_dicts

        diff_result = compare_schemas_from_dicts(
            baseline_result.data["schema"],
            live_result.data["schema"],
        )
        if not diff_result.success:
            return ToolResult(success=False, error=diff_result.error)

        return ToolResult(
            success=True,
            data={
                "schema_name": self.config.schema_name,
                "baseline_version": baseline_result.data["version"],
                "diff": diff_result.data["diff"],
            },
        )


def _substitute(template: str, env_names: list[str]) -> str:
    """Replace every ``$NAME`` in *template* with the corresponding env var value."""
    for name in env_names:
        template = template.replace(f"${name}", os.environ.get(name, f"${name}"))
    return template
