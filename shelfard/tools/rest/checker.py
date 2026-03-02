"""RestChecker — runs a drift check against a live REST endpoint."""

import os

from ...models import RestCheckerConfig, ToolResult
from .reader import RestEndpointReader
from ...schema_comparison import compare_schemas_from_dicts
from ..base import Checker


class RestChecker(Checker):
    def __init__(self, config: RestCheckerConfig, registry) -> None:
        self.config = config
        self.registry = registry  # SchemaRegistry instance

    def run(self) -> ToolResult:
        # 1. Validate all required env vars are present
        missing = [v for v in self.config.env if not os.environ.get(v)]
        if missing:
            return ToolResult(
                success=False,
                error=f"Missing required env vars: {', '.join(missing)}",
            )

        # 2. Resolve $VAR substitutions in URL and header values
        url = _substitute(self.config.url, self.config.env)
        resolved_headers: dict[str, str] = {}
        for entry in self.config.headers:
            for k, v in entry.items():
                resolved_headers[k] = _substitute(v, self.config.env)

        # 3. Fetch current schema from the live endpoint
        new_result = RestEndpointReader(
            url, self.config.schema_name, headers=resolved_headers or None
        ).get_schema()
        if not new_result.success:
            return ToolResult(success=False, error=f"Fetch failed: {new_result.error}")

        # 4. Load the saved baseline
        baseline_result = self.registry.get_registered_schema(self.config.schema_name)
        if not baseline_result.success:
            return ToolResult(
                success=False,
                error=(
                    f"No snapshot for '{self.config.schema_name}'. "
                    f"Run: shelfard rest snapshot {url} --name {self.config.schema_name}"
                ),
            )

        # 5. Diff
        diff_result = compare_schemas_from_dicts(
            baseline_result.data["schema"], new_result.data["schema"]
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
    """Replace $VAR_NAME with os.environ[VAR_NAME] for each name in env_names."""
    result = template
    for name in env_names:
        result = result.replace(f"${name}", os.environ.get(name, f"${name}"))
    return result
