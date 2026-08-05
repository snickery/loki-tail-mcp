from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx

from loki_tail_mcp.config import (
    DEFAULT_LIMIT,
    LOKI_BASIC_AUTH,
    LOKI_TENANT_ID,
    LOKI_TIMEOUT,
    LOKI_URL,
    MAX_LIMIT,
)


class LokiError(RuntimeError):
    """Raised when Loki returns a non-2xx response."""


_DURATION_RE = re.compile(r"^(\d+)\s*(ns|us|µs|ms|s|m|h|d|w)$")
_DURATION_UNITS_NS = {
    "ns": 1,
    "us": 1_000,
    "µs": 1_000,
    "ms": 1_000_000,
    "s": 1_000_000_000,
    "m": 60 * 1_000_000_000,
    "h": 3600 * 1_000_000_000,
    "d": 86400 * 1_000_000_000,
    "w": 7 * 86400 * 1_000_000_000,
}


def _now_ns() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1e9)


def parse_time(value: str | None, *, default_offset_s: int = 0) -> int:
    """Parse a user-supplied time into nanoseconds since epoch.

    Accepts:
      - None / "" / "now"          → now (+ default_offset_s)
      - "15m", "2h", "3d"          → now minus that duration
      - "-15m", "-2h"              → same, explicit minus
      - RFC3339 / ISO8601 string   → parsed absolute time
      - Pure digit string          → assumed to already be ns since epoch
    """
    if value is None or value == "" or value == "now":
        return _now_ns() + default_offset_s * 1_000_000_000

    s = value.strip()
    if s.startswith("-"):
        s = s[1:]
    m = _DURATION_RE.match(s)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        delta_ns = n * _DURATION_UNITS_NS[unit]
        return _now_ns() - delta_ns

    if s.isdigit():
        return int(s)

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1e9)
    except ValueError as e:
        raise ValueError(
            f"Could not parse time '{value}'. Use durations like '15m', "
            "RFC3339 timestamps, or ns-since-epoch integers."
        ) from e


def clamp_limit(limit: int | None) -> int:
    if limit is None or limit <= 0:
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


class LokiClient:
    def __init__(self, base_url: str = LOKI_URL, timeout: float = LOKI_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        headers = {"X-Scope-OrgID": LOKI_TENANT_ID} if LOKI_TENANT_ID else {}
        auth = None
        if LOKI_BASIC_AUTH:
            user, _, password = LOKI_BASIC_AUTH.partition(":")
            auth = (user, password)
        self._client = httpx.Client(timeout=timeout, headers=headers, auth=auth)

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        clean = {k: v for k, v in params.items() if v is not None}
        resp = self._client.get(url, params=clean)
        if resp.status_code != 200:
            raise LokiError(
                f"Loki {path} returned {resp.status_code}: {resp.text[:500]}"
            )
        return resp.json()

    def query_range(
        self,
        query: str,
        start_ns: int,
        end_ns: int,
        limit: int,
        direction: str = "backward",
        step: str | None = None,
    ) -> dict[str, Any]:
        return self._get(
            "/loki/api/v1/query_range",
            {
                "query": query,
                "start": str(start_ns),
                "end": str(end_ns),
                "limit": limit,
                "direction": direction,
                "step": step,
            },
        )

    def query_instant(
        self, query: str, time_ns: int, limit: int, direction: str = "backward"
    ) -> dict[str, Any]:
        return self._get(
            "/loki/api/v1/query",
            {
                "query": query,
                "time": str(time_ns),
                "limit": limit,
                "direction": direction,
            },
        )

    def labels(self, start_ns: int, end_ns: int) -> list[str]:
        data = self._get(
            "/loki/api/v1/labels",
            {"start": str(start_ns), "end": str(end_ns)},
        )
        return data.get("data", [])

    def label_values(self, name: str, start_ns: int, end_ns: int) -> list[str]:
        data = self._get(
            f"/loki/api/v1/label/{quote(name, safe='')}/values",
            {"start": str(start_ns), "end": str(end_ns)},
        )
        return data.get("data", [])

    def patterns(self, query: str, start_ns: int, end_ns: int) -> list[dict[str, Any]]:
        """Mine log patterns for a selector (requires pattern_ingester on
        the Loki server; 404 there surfaces as a LokiError). Only logs
        ingested after enablement are mined."""
        data = self._get(
            "/loki/api/v1/patterns",
            {"query": query, "start": str(start_ns), "end": str(end_ns)},
        )
        return data.get("data", [])

    def index_volume(
        self, query: str, start_ns: int, end_ns: int, limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Log-volume (bytes) per label combination for a selector."""
        data = self._get(
            "/loki/api/v1/index/volume",
            {
                "query": query,
                "start": str(start_ns),
                "end": str(end_ns),
                "limit": limit,
            },
        )
        return data.get("data", {}).get("result", [])

    def detected_fields(
        self, query: str, start_ns: int, end_ns: int, limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Fields Loki can auto-extract from a stream (name, type,
        cardinality, parser)."""
        data = self._get(
            "/loki/api/v1/detected_fields",
            {
                "query": query,
                "start": str(start_ns),
                "end": str(end_ns),
                "limit": limit,
            },
        )
        return data.get("fields", [])


def format_streams(result: dict[str, Any], include_labels: list[str] | None = None) -> str:
    """Render a Loki streams response as compact newline-separated log lines.

    Each line is: `ISO8601  [label=value label=value]  message`
    Only labels in `include_labels` are surfaced (default: container, service_name, level).
    """
    if include_labels is None:
        include_labels = ["container", "service_name", "level"]

    data = result.get("data", {})
    result_type = data.get("resultType")
    streams = data.get("result", [])

    if result_type == "matrix":
        return _format_matrix(streams)

    if not streams:
        return "(no results)"

    rows: list[tuple[int, str]] = []
    for stream in streams:
        labels = stream.get("stream", {})
        label_str = " ".join(
            f"{k}={labels[k]}" for k in include_labels if k in labels
        )
        for ts_ns_str, line in stream.get("values", []):
            try:
                ts_ns = int(ts_ns_str)
            except (TypeError, ValueError):
                continue
            ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).isoformat(
                timespec="milliseconds"
            )
            prefix = f"{ts}  [{label_str}]  " if label_str else f"{ts}  "
            rows.append((ts_ns, prefix + line.rstrip()))

    rows.sort(key=lambda r: r[0], reverse=True)
    return "\n".join(r[1] for r in rows)


def _format_matrix(series: list[dict[str, Any]]) -> str:
    if not series:
        return "(no results)"
    out = []
    for s in series:
        labels = s.get("metric") or s.get("stream") or {}
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        values = s.get("values", [])
        samples = " ".join(f"{ts}:{v}" for ts, v in values)
        out.append(f"{{{label_str}}} {samples}")
    return "\n".join(out)
