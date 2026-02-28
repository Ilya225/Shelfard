"""
Integration tests for RestEndpointReader.

Uses a local mock HTTP server (stdlib http.server + socketserver) so no real
network is needed. Each test starts its own server on a random OS-assigned port
and shuts it down in a finally block.

Run: conda run -n shelfard python3 tests/test_rest_reader.py
"""

import http.server
import json
import socketserver
import sys
import threading
import traceback
from pathlib import Path

# Make the project root importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools import ColumnType, RestEndpointReader, get_rest_schema

# ─────────────────────────────────────────────
# Minimal test framework (mirrors run_tests.py)
# ─────────────────────────────────────────────

passed = 0
failed = 0
errors = []


def test(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  ✓ {name}")
        passed += 1
    except AssertionError as e:
        print(f"  ✗ {name}")
        errors.append((name, str(e)))
        failed += 1
    except Exception as e:
        print(f"  ✗ {name} [ERROR]")
        errors.append((name, traceback.format_exc()))
        failed += 1


def section(name):
    print(f"\n── {name} ──")


# ─────────────────────────────────────────────
# Mock HTTP server helpers
# ─────────────────────────────────────────────

def make_mock_server(response_body, *, status: int = 200, content_type: str = "application/json"):
    """
    Start a single-response local HTTP server on a random port.

    The handler records the last request's headers on Handler.last_headers so
    tests can assert which headers were sent.

    Returns (httpd, port). Caller is responsible for calling httpd.shutdown().
    """
    body_bytes = (
        json.dumps(response_body).encode()
        if not isinstance(response_body, (bytes, str))
        else (response_body.encode() if isinstance(response_body, str) else response_body)
    )

    class Handler(http.server.BaseHTTPRequestHandler):
        last_headers: dict = {}

        def do_GET(self):
            Handler.last_headers = dict(self.headers)
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body_bytes)))
            self.end_headers()
            self.wfile.write(body_bytes)

        def log_message(self, *args):
            pass  # suppress server output during tests

    httpd = socketserver.TCPServer(("127.0.0.1", 0), Handler)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever)
    thread.daemon = True
    thread.start()
    return httpd, port, Handler


# ─────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────

section("REST Endpoint Reader")


def test_basic_fetch():
    httpd, port, _ = make_mock_server({"id": 1, "name": "Alice", "active": True})
    try:
        result = RestEndpointReader(f"http://127.0.0.1:{port}/users/1", "user").get_schema()
        assert result.success, result.error
        cols = {c["name"]: c for c in result.data["schema"]["columns"]}
        assert cols["id"]["col_type"]     == ColumnType.INTEGER
        assert cols["name"]["col_type"]   == ColumnType.VARCHAR
        assert cols["active"]["col_type"] == ColumnType.BOOLEAN
        assert result.data["schema"]["source"] == "rest_api"
        assert result.data["schema"]["table_name"] == "user"
    finally:
        httpd.shutdown()


test("basic fetch infers INTEGER, VARCHAR, BOOLEAN fields", test_basic_fetch)


def test_bearer_token_sent():
    httpd, port, Handler = make_mock_server({"ok": True})
    try:
        RestEndpointReader(
            f"http://127.0.0.1:{port}/secure",
            "secure",
            bearer_token="my-secret-token",
        ).get_schema()
        auth = Handler.last_headers.get("Authorization", "")
        assert auth == "Bearer my-secret-token", f"Got: {auth!r}"
    finally:
        httpd.shutdown()


test("bearer_token= sets Authorization: Bearer <token> header", test_bearer_token_sent)


def test_custom_header_sent():
    httpd, port, Handler = make_mock_server({"ok": True})
    try:
        RestEndpointReader(
            f"http://127.0.0.1:{port}/api",
            "api",
            headers={"X-Api-Key": "abc123", "X-Tenant": "acme"},
        ).get_schema()
        assert Handler.last_headers.get("X-Api-Key") == "abc123"
        assert Handler.last_headers.get("X-Tenant") == "acme"
    finally:
        httpd.shutdown()


test("custom headers= dict is forwarded in the request", test_custom_header_sent)


def test_array_response_uses_first_element():
    httpd, port, _ = make_mock_server([{"id": 1, "email": "a@b.com"}, {"id": 2, "email": "c@d.com"}])
    try:
        result = RestEndpointReader(f"http://127.0.0.1:{port}/users", "users").get_schema()
        assert result.success, result.error
        cols = {c["name"]: c for c in result.data["schema"]["columns"]}
        assert cols["id"]["col_type"]    == ColumnType.INTEGER
        assert cols["email"]["col_type"] == ColumnType.VARCHAR
    finally:
        httpd.shutdown()


test("array response → schema inferred from first element", test_array_response_uses_first_element)


def test_non_200_returns_error():
    httpd, port, _ = make_mock_server({"detail": "not found"}, status=404)
    try:
        result = RestEndpointReader(f"http://127.0.0.1:{port}/missing", "x").get_schema()
        assert not result.success
        assert "404" in result.error, f"Expected '404' in error, got: {result.error!r}"
    finally:
        httpd.shutdown()


test("HTTP 404 → success=False with status code in error", test_non_200_returns_error)


def test_non_json_response_returns_error():
    httpd, port, _ = make_mock_server(
        "<html>Not JSON</html>",
        content_type="text/html",
    )
    try:
        result = RestEndpointReader(f"http://127.0.0.1:{port}/html", "x").get_schema()
        assert not result.success
        assert "json" in result.error.lower(), f"Expected 'json' in error, got: {result.error!r}"
    finally:
        httpd.shutdown()


test("non-JSON response → success=False mentioning JSON", test_non_json_response_returns_error)


def test_list_tables_not_supported():
    reader = RestEndpointReader("http://example.com/api/users", "users")
    result = reader.list_tables()
    assert not result.success
    assert result.next_action_hint is not None


test("list_tables() returns success=False with hint", test_list_tables_not_supported)


# ─────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────

total = passed + failed
print(f"\n{'='*50}")
print(f"Results: {passed}/{total} passed", end="")
if failed:
    print(f"  ({failed} failed)")
    print("\nFailures:")
    for name, err in errors:
        print(f"\n  ✗ {name}")
        print(f"    {err}")
else:
    print(" ✓")
print("=" * 50)
sys.exit(0 if failed == 0 else 1)
