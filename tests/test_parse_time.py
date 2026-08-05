from datetime import datetime, timezone

import pytest

from loki_tail_mcp.loki_client import clamp_limit, parse_time


def _now_ns() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1e9)


def test_parse_time_none_is_now():
    before = _now_ns()
    got = parse_time(None)
    after = _now_ns()
    assert before <= got <= after


def test_parse_time_empty_is_now():
    assert abs(parse_time("") - _now_ns()) < 1e9  # within 1s


def test_parse_time_now_literal():
    assert abs(parse_time("now") - _now_ns()) < 1e9


@pytest.mark.parametrize(
    "expr,seconds",
    [
        ("15s", 15),
        ("5m", 300),
        ("2h", 7200),
        ("1d", 86400),
        ("1w", 7 * 86400),
    ],
)
def test_parse_time_durations(expr, seconds):
    now = _now_ns()
    got = parse_time(expr)
    delta_ns = now - got
    expected_ns = seconds * 1_000_000_000
    # within 1 second of expected offset
    assert abs(delta_ns - expected_ns) < 1_000_000_000


def test_parse_time_explicit_minus():
    a = parse_time("15m")
    b = parse_time("-15m")
    assert abs(a - b) < 1_000_000_000


def test_parse_time_rfc3339():
    got = parse_time("2025-01-01T00:00:00Z")
    expected = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9)
    assert got == expected


def test_parse_time_naive_iso_assumes_utc():
    got = parse_time("2025-01-01T00:00:00")
    expected = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9)
    assert got == expected


def test_parse_time_ns_passthrough():
    assert parse_time("1735689600000000000") == 1735689600000000000


def test_parse_time_bad_string_raises():
    with pytest.raises(ValueError):
        parse_time("not a time")


def test_clamp_limit_defaults():
    assert clamp_limit(None) == 100
    assert clamp_limit(0) == 100
    assert clamp_limit(-5) == 100


def test_clamp_limit_respects_max():
    assert clamp_limit(50) == 50
    assert clamp_limit(10_000) == 1000
