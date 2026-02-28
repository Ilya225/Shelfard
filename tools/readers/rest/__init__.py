"""
REST API schema reader.

RestEndpointReader fetches a live HTTP endpoint and infers a TableSchema
from the JSON response body — useful for detecting drift in API payloads
over time.

infer_schema_from_openapi() is planned but not yet implemented.
"""

from datetime import datetime

import requests

from ..base import SchemaReader
from ...models import TableSchema, ToolResult
from ...parsers.json_file_reader import _build_column_schema


class RestEndpointReader(SchemaReader):
    """
    Reads a schema from a live REST API endpoint.

    The URL, schema name, and auth credentials are all fixed at construction
    time so that get_schema() requires no arguments, matching the SchemaReader
    contract.
    """

    def __init__(
        self,
        url: str,
        schema_name: str,
        *,
        bearer_token: str | None = None,
        headers: dict | None = None,
    ):
        """
        Args:
            url:          Endpoint to GET (must return a JSON object or array of objects).
            schema_name:  Logical name for the schema (used as table_name in TableSchema).
            bearer_token: Convenience param — sets Authorization: Bearer <token>.
            headers:      Arbitrary extra headers; merged last so they override bearer_token.
        """
        self.url = url
        self.schema_name = schema_name
        self._headers: dict[str, str] = {}
        if bearer_token:
            self._headers["Authorization"] = f"Bearer {bearer_token}"
        if headers:
            self._headers.update(headers)

    def get_schema(self) -> ToolResult:
        """
        Fetches the endpoint and infers a TableSchema from the JSON response.

        Supports both object and array-of-objects responses (uses first element).
        Nested dicts are inferred as STRUCT columns recursively.
        """
        try:
            response = requests.get(self.url, headers=self._headers, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            return ToolResult(
                success=False,
                error=f"HTTP {e.response.status_code}: {e.response.reason}",
                next_action_hint="Check the endpoint URL and authentication credentials.",
            )
        except requests.exceptions.ConnectionError as e:
            return ToolResult(
                success=False,
                error=f"Connection failed: {e}",
                next_action_hint="Verify the URL is reachable and the network is available.",
            )
        except requests.exceptions.Timeout:
            return ToolResult(
                success=False,
                error="Request timed out after 30 seconds.",
                next_action_hint="Check if the endpoint is responsive.",
            )

        try:
            data = response.json()
        except (ValueError, requests.exceptions.JSONDecodeError) as e:
            return ToolResult(
                success=False,
                error=f"Response is not valid JSON: {e}",
                next_action_hint="Ensure the endpoint returns application/json.",
            )

        if isinstance(data, list):
            if not data:
                return ToolResult(
                    success=False,
                    error="Response is an empty JSON array — cannot infer schema.",
                    next_action_hint="Try an endpoint that returns at least one record.",
                )
            data = data[0]

        if not isinstance(data, dict):
            return ToolResult(
                success=False,
                error=f"Expected a JSON object or array of objects, got {type(data).__name__}.",
            )

        columns = [_build_column_schema(k, v) for k, v in data.items()]
        schema = TableSchema(
            table_name=self.schema_name,
            columns=columns,
            source="rest_api",
            captured_at=datetime.utcnow().isoformat(),
        )

        return ToolResult(
            success=True,
            data={"schema": schema.to_dict()},
            next_action_hint="Call get_registered_schema() and compare_schemas() to detect drift.",
        )

    def list_tables(self) -> ToolResult:
        """REST sources don't expose a table listing — not applicable."""
        return ToolResult(
            success=False,
            error="REST sources do not support table listing.",
            next_action_hint=(
                "Instantiate RestEndpointReader with a specific endpoint URL "
                "and schema_name, then call get_schema()."
            ),
        )


# ── Module-level convenience wrapper ──────────────────────────────────────

def get_rest_schema(url: str, schema_name: str, **kwargs) -> ToolResult:
    """Fetch a REST endpoint and infer its schema. Keyword args forwarded to RestEndpointReader."""
    return RestEndpointReader(url, schema_name, **kwargs).get_schema()


def infer_schema_from_openapi(url_or_path: str, resource_name: str) -> ToolResult:
    """Parse a Swagger/OpenAPI spec and extract a named resource as a TableSchema.
    Not yet implemented."""
    return ToolResult(
        success=False,
        error="OpenAPI reader is not yet implemented.",
        next_action_hint="Provide an OpenAPI spec path and a resource name once implemented.",
    )
