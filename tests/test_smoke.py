"""Smoke test: API surface + SSE parsing against a mock urlopen (no network)."""

import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import crtrs.driver as crtrs_driver
from crtrs.driver import Driver, DriverError


class FakeResponse(io.BytesIO):
    """Byte stream that also works as a context manager (like an HTTP response)."""

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


def make_urlopen(captured, body, expect_prompt):
    def fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["auth"] = req.headers.get("Authorization")
        assert json.loads(req.data) == {"prompt": expect_prompt}
        return FakeResponse(body.encode("utf-8"))

    return fake_urlopen


def test_requires_api_key():
    os.environ.pop("DRIVER_API_KEY", None)
    try:
        Driver()
    except ValueError as e:
        assert "missing api_key" in str(e)
    else:
        raise AssertionError("should require an api_key")


def test_stream_allowlist():
    captured = {}
    body = (
        'data: {"kind":"plan","items":["a","b"]}\n\n'
        'data: {"kind":"step","def":"internal leak"}\n\n'   # hidden kind: dropped
        "data: not-json should be dropped\n\n"                # non-JSON: dropped
        'data: {"kind":"action","tool":"http fetching","is_network":true}\n\n'
        'data: {"kind":"done","result":"ok","steps":2,"errors":0}\n\n'
    )
    crtrs_driver.urllib.request.urlopen = make_urlopen(captured, body, "hi")

    driver = Driver(api_key="dr_test")
    kinds = [ev["kind"] for ev in driver.stream("hi")]

    assert captured["url"].endswith("/api/driver/run")
    assert captured["auth"] == "Bearer dr_test"
    assert kinds == ["plan", "action", "done"], kinds


def test_fatal_raises():
    body = 'data: {"kind":"fatal","semantic":"provider"}\n\n'
    crtrs_driver.urllib.request.urlopen = make_urlopen({}, body, "boom")
    driver = Driver(api_key="dr_test")
    try:
        list(driver.stream("boom"))
    except DriverError as e:
        assert str(e) == "provider"
    else:
        raise AssertionError("fatal should raise DriverError")


if __name__ == "__main__":
    test_requires_api_key()
    test_stream_allowlist()
    test_fatal_raises()
    print("ok — crtrs-driver smoke test passed")
