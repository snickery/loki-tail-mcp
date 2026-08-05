import functools
import hmac
import inspect
import json
import logging
import os
import time

import anyio
from mcp.server.fastmcp import FastMCP

from loki_tail_mcp.config import MCP_BEARER_TOKEN, PORT
from loki_tail_mcp.loki_client import (
    LokiClient,
    LokiError,
    clamp_limit,
    format_streams,
    parse_time,
)
from loki_tail_mcp.resolve import filter_ephemeral, resolve_container

logger = logging.getLogger(__name__)

INSTRUCTIONS = (
    "Loki log search across the container fleet. "
    "Start with loki_tail_container for 'what is service X saying' — it "
    "accepts approximate names (service vocabulary like 'vpn', bare names "
    "like 'sonarr') and on a miss either auto-corrects (flagged in the "
    "output) or suggests candidates; retry a suggestion exactly as given. "
    "Keep windows narrow (minutes<=15) and pass a substring filter (e.g. "
    "'ERROR') whenever the question hints at one — unfiltered wide queries "
    "waste the call on truncated output. Use loki_query_range with raw "
    'LogQL only for multi-container correlation ({container=~"a|b"} |= '
    '"...") or metric queries (count_over_time(...)). Discovery: '
    "loki_list_containers lists durable container names (ephemeral "
    "CI/batch names are hidden; loki_list_label_values("
    "'container') returns the raw list). Useful labels: container, "
    "service_name, host, level. Timestamps are UTC."
)

mcp = FastMCP(
    "loki",
    instructions=INSTRUCTIONS,
    host="0.0.0.0",
    port=PORT,
    stateless_http=True,
    json_response=True,
)
loki = LokiClient()


# --- Run blocking tool handlers off the event loop -----------------------
# The MCP SDK runs a synchronous @mcp.tool function INLINE on the single
# asyncio event loop (func_metadata: the sync branch is just `return fn(...)`).
# Every tool here makes a blocking httpx call to Loki, so one slow query would
# freeze the loop and stall ALL other requests until it returns (the client's
# 4-minute "MCP server unresponsive" timeout). Wrapping mcp.tool runs each
# sync tool in a worker thread via anyio.to_thread.run_sync, keeping the loop
# free. functools.wraps + __signature__ keep the tool schema unchanged (no
# client restart); we register the wrapper but return the original sync fn so
# the module name stays directly callable. httpx.Client is thread-safe, so the
# shared LokiClient needs no lock. See feedback_mcp_sync_tool_event_loop.
req_logger = logging.getLogger("loki_tail_mcp.requests")
if not req_logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    req_logger.addHandler(_h)
    req_logger.setLevel(logging.INFO)
    req_logger.propagate = False


def _rss_mib() -> int:
    """Current process RSS in MiB, read cheaply from /proc (no psutil dep) —
    lets Grafana correlate a hang/OOM with the request that triggered it."""
    try:
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
        return pages * (os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf")
                        else 4096) // (1024 * 1024)
    except Exception:
        return -1


_orig_tool = mcp.tool


def _threaded_tool(*d_args, **d_kwargs):
    register = _orig_tool(*d_args, **d_kwargs)

    def decorator(fn):
        if inspect.iscoroutinefunction(fn):
            return register(fn)

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            outcome = "ok"
            try:
                return await anyio.to_thread.run_sync(
                    functools.partial(fn, *args, **kwargs)
                )
            except Exception:
                outcome = "error"
                raise
            finally:
                req_logger.info(
                    "tool=%s outcome=%s duration_ms=%d rss_mib=%d",
                    fn.__name__, outcome,
                    int((time.perf_counter() - start) * 1000),
                    _rss_mib(),
                )

        wrapper.__signature__ = inspect.signature(fn)
        register(wrapper)
        return fn

    return decorator


mcp.tool = _threaded_tool


class BearerAuthMiddleware:
    """Pure ASGI middleware compatible with SSE streaming."""

    def __init__(self, app, expected_token: str):
        self.app = app
        self.expected_token = expected_token

    async def __call__(self, scope, receive, send):
        if scope["type"] in ("http", "websocket"):
            path = scope.get("path", "")
            if path.startswith("/.well-known/"):
                return await self.app(scope, receive, send)
            headers = dict(scope.get("headers", []))
            auth = headers.get(b"authorization", b"").decode()
            if auth.startswith("Bearer "):
                token = auth[7:]
                if hmac.compare_digest(token, self.expected_token):
                    return await self.app(scope, receive, send)
            client_host = scope.get("client", ("unknown",))[0]
            logger.warning("Rejected request: invalid bearer from %s", client_host)
            body = json.dumps({"error": "unauthorized"}).encode()
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        [b"content-type", b"application/json"],
                        [b"content-length", str(len(body)).encode()],
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return
        await self.app(scope, receive, send)


# ---------------------------------------------------------------------------
# Query tools
# ---------------------------------------------------------------------------


@mcp.tool()
def loki_query_range(
    query: str,
    start: str = "1h",
    end: str = "now",
    limit: int = 100,
    direction: str = "backward",
) -> str:
    """Run a LogQL range query against Loki.

    query:    LogQL expression, e.g. `{container="nginx"} |= "ERROR"`
              or a metric query like `rate({container="traefik"}[5m])`.
    start:    Duration back from now (`15m`, `2h`, `3d`), RFC3339 timestamp,
              or ns-since-epoch. Default `1h`.
    end:      Same format as start. Default `now`.
    limit:    Max log lines to return (clamped to server max, default 100).
    direction: `backward` (newest first) or `forward`. Default backward.

    Returns compact `timestamp  [labels]  message` lines, newest first. For
    metric queries returns `{labels} ts:val ts:val ...` per series.
    """
    try:
        start_ns = parse_time(start)
        end_ns = parse_time(end)
        if end_ns <= start_ns:
            return "Error: end must be after start."
        result = loki.query_range(
            query, start_ns, end_ns, clamp_limit(limit), direction
        )
        return format_streams(result)
    except (LokiError, ValueError) as e:
        return f"Error: {e}"


@mcp.tool()
def loki_query_instant(
    query: str,
    time: str = "now",
    limit: int = 100,
) -> str:
    """Run a LogQL instant query at a single point in time.

    Best for metric queries like `count_over_time({container="crowdsec"}[5m])`.
    For raw log search, prefer `loki_query_range`.
    """
    try:
        t_ns = parse_time(time)
        result = loki.query_instant(query, t_ns, clamp_limit(limit))
        return format_streams(result)
    except (LokiError, ValueError) as e:
        return f"Error: {e}"


@mcp.tool()
def loki_tail_container(
    container: str,
    minutes: int = 15,
    limit: int = 200,
    filter: str = "",
) -> str:
    """Tail the most recent log lines for a single container.

    container: Container name (e.g. `nginx`, `traefik`, `arr-sonarr`).
               Exact names work best; an unknown name is resolved against
               live container names (service vocabulary like `vpn` works) —
               a unique match is tailed automatically and flagged in the
               output, otherwise candidate names are returned.
    minutes:   How far back to look. Default 15.
    limit:     Max lines to return. Default 200.
    filter:    Optional substring filter applied as LogQL `|= "filter"`.
               Case-sensitive. Leave empty for no filter.
    """

    def tail(name: str) -> str:
        # Escape the container name exactly like the filter string — an
        # unescaped quote in `name` would widen the query past one container.
        escaped_name = name.replace("\\", "\\\\").replace('"', '\\"')
        query = f'{{container="{escaped_name}"}}'
        if filter:
            escaped = filter.replace("\\", "\\\\").replace('"', '\\"')
            query = f'{query} |= "{escaped}"'
        end_ns = parse_time("now")
        start_ns = parse_time(f"{minutes}m")
        result = loki.query_range(
            query, start_ns, end_ns, clamp_limit(limit), "backward"
        )
        return format_streams(result)

    try:
        out = tail(container)
        if out != "(no results)":
            return out
        # Zero results: distinguish a wrong container name from a valid but
        # quiet one, so a single-shot LLM caller can self-correct without
        # spending another tool call on discovery.
        names, _ = filter_ephemeral(
            loki.label_values("container", parse_time("24h"), parse_time("now"))
        )
        filter_note = " matching the filter" if filter else ""
        if container in names:
            return (
                f"(no results) '{container}' has logged within 24h but has no "
                f"lines{filter_note} in the last {minutes}m — try a larger "
                f"`minutes`{' or drop the `filter`' if filter else ''}."
            )
        match, suggestions = resolve_container(container, names)
        if match:
            note = f"[no container '{container}' — resolved to '{match}']"
            out = tail(match)
            if out == "(no results)":
                return (
                    f"{note} (no lines{filter_note} in the last {minutes}m — "
                    f"try a larger `minutes`)"
                )
            return f"{note}\n{out}"
        if suggestions:
            return (
                f"No container named '{container}'. Closest live container "
                f"names: {', '.join(suggestions)}. Retry with one of those."
            )
        return (
            f"No container named '{container}'. Containers logging in the "
            f"last 24h: {', '.join(sorted(names))}"
        )
    except (LokiError, ValueError) as e:
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Discovery tools
# ---------------------------------------------------------------------------


@mcp.tool()
def loki_list_labels(lookback: str = "1h") -> str:
    """List all label names Loki has seen in the given lookback window.

    lookback: Duration (`1h`, `24h`, `7d`). Default `1h`. Shorter is faster.
    """
    try:
        end_ns = parse_time("now")
        start_ns = parse_time(lookback)
        labels = loki.labels(start_ns, end_ns)
        return "\n".join(labels) if labels else "(no labels in window)"
    except (LokiError, ValueError) as e:
        return f"Error: {e}"


@mcp.tool()
def loki_list_label_values(label: str, lookback: str = "1h") -> str:
    """List all values seen for a given label in the lookback window.

    label:    Label name (e.g. `container`, `service_name`, `job`, `host`).
    lookback: Duration (`1h`, `24h`, `7d`). Default `1h`.
    """
    try:
        end_ns = parse_time("now")
        start_ns = parse_time(lookback)
        values = loki.label_values(label, start_ns, end_ns)
        return "\n".join(sorted(values)) if values else "(no values in window)"
    except (LokiError, ValueError) as e:
        return f"Error: {e}"


@mcp.tool()
def loki_patterns(container: str, minutes: int = 60) -> str:
    """Mine a container's recent logs into recurring patterns — the fastest
    way to summarize what a noisy service is saying. One call turns
    thousands of lines into a ranked list of templates (`<_>` marks the
    variable parts) with occurrence counts.

    container: exact container name (use loki_list_containers /
               loki_tail_container to find it).
    minutes:   how far back to mine. Default 60.

    Requires the Loki pattern ingester; only logs ingested after it was
    enabled are mined, so a small window right after enablement is normal.
    """
    escaped = container.replace("\\", "\\\\").replace('"', '\\"')
    try:
        end_ns = parse_time("now")
        start_ns = parse_time(f"{minutes}m")
        pats = loki.patterns(f'{{container="{escaped}"}}', start_ns, end_ns)
    except (LokiError, ValueError) as e:
        return f"Error: {e}"
    if not pats:
        return (
            f"No patterns for '{container}' in the last {minutes}m — the "
            "container may be quiet, the name wrong (loki_list_containers), "
            "or the pattern ingester recently enabled (it only mines logs "
            "ingested after enablement)."
        )
    ranked = sorted(
        pats, key=lambda x: sum(c for _, c in x.get("samples", [])), reverse=True,
    )[:30]
    lines = [f"{len(ranked)} pattern(s) for '{container}' (last {minutes}m):\n"]
    for pat in ranked:
        total = sum(c for _, c in pat.get("samples", []))
        level = pat.get("level") or "-"
        lines.append(f"  {total:>7}x [{level}] {pat.get('pattern', '')}")
    return "\n".join(lines)


@mcp.tool()
def loki_log_volume(minutes: int = 60, limit: int = 10, container: str = "") -> str:
    """Rank containers by log volume (bytes) — "which service suddenly got
    noisy". Leave container empty to rank the whole fleet; set it to scope
    to one container.

    minutes: window to measure. Default 60.
    limit:   max rows. Default 10.
    """
    escaped = container.replace("\\", "\\\\").replace('"', '\\"')
    selector = f'{{container="{escaped}"}}' if container else '{container=~".+"}'
    try:
        end_ns = parse_time("now")
        start_ns = parse_time(f"{minutes}m")
        rows = loki.index_volume(selector, start_ns, end_ns, limit=max(1, limit))
    except (LokiError, ValueError) as e:
        return f"Error: {e}"
    if not rows:
        return f"No log volume recorded in the last {minutes}m."
    lines = [f"Log volume, last {minutes}m:\n"]
    for r in rows:
        labels = r.get("metric", {})
        name = labels.get("container") or ", ".join(
            f"{k}={v}" for k, v in sorted(labels.items())
        ) or "(no labels)"
        try:
            nbytes = int(float(r.get("value", [0, "0"])[1]))
        except (TypeError, ValueError, IndexError):
            nbytes = 0
        if nbytes >= 1024 * 1024:
            human = f"{nbytes / (1024 * 1024):.1f} MiB"
        elif nbytes >= 1024:
            human = f"{nbytes / 1024:.1f} KiB"
        else:
            human = f"{nbytes} B"
        lines.append(f"  {human:>10}  {name}")
    return "\n".join(lines)


@mcp.tool()
def loki_detected_fields(container: str, minutes: int = 60, limit: int = 20) -> str:
    """Discover the fields Loki can auto-extract from a container's logs
    (name, type, cardinality, parser — e.g. logfmt/json). Use this before
    writing a LogQL parse stage in loki_query_range, e.g. finding that a
    service logs logfmt with a `status` field enables
    `{container="x"} | logfmt | status>=500`.

    container: exact container name.
    minutes:   window to sample. Default 60.
    limit:     max fields. Default 20.
    """
    escaped = container.replace("\\", "\\\\").replace('"', '\\"')
    try:
        end_ns = parse_time("now")
        start_ns = parse_time(f"{minutes}m")
        fields = loki.detected_fields(
            f'{{container="{escaped}"}}', start_ns, end_ns, limit=max(1, limit),
        )
    except (LokiError, ValueError) as e:
        return f"Error: {e}"
    if not fields:
        return (
            f"No extractable fields detected for '{container}' in the last "
            f"{minutes}m (unstructured logs, a quiet container, or a wrong "
            "name — see loki_list_containers)."
        )
    lines = [f"{len(fields)} field(s) detected for '{container}' (last {minutes}m):\n"]
    for f in fields:
        parsers = ",".join(f.get("parsers") or []) or "index/metadata"
        lines.append(
            f"  {f.get('label', '?'):<24} {f.get('type', '?'):<8} "
            f"cardinality={f.get('cardinality', '?'):<6} via {parsers}"
        )
    return "\n".join(lines)


@mcp.tool()
def loki_list_containers(lookback: str = "1h") -> str:
    """List durable container names actively logging in the lookback window.

    Hides ephemeral noise — podman auto-names (`adoring_neumann`) and
    hex-suffixed batch workers (`transcode-ffmpeg-<hash>`) number in the
    thousands over 24h and would swamp the response. They remain queryable
    via `loki_query_range`; use `loki_list_label_values('container')` for
    the raw unfiltered list.

    lookback: Duration (`1h`, `24h`, `7d`). Default `1h`.
    """
    try:
        end_ns = parse_time("now")
        start_ns = parse_time(lookback)
        values = loki.label_values("container", start_ns, end_ns)
        names, hidden = filter_ephemeral(values)
        if not names:
            return "(no containers in window)"
        out = "\n".join(sorted(names))
        if hidden:
            out += f"\n({hidden} ephemeral auto-named/batch containers hidden)"
        return out
    except (LokiError, ValueError) as e:
        return f"Error: {e}"


def main():
    import sys

    # Emit our per-request tool logs at INFO so they reach journald → Loki.
    # basicConfig writes to stderr, so this is safe for the stdio transport
    # too (stdout must stay pure JSON-RPC).
    logging.basicConfig(level=logging.INFO)

    if "--stdio" in sys.argv:
        # Local single-client mode: the MCP client spawns this process and
        # owns it — no network surface, so no bearer token or uvicorn.
        mcp.run(transport="stdio")
        return

    import uvicorn

    # Streamable HTTP at /mcp, stateless: every tool call is an independent
    # POST, so dropped connections and container restarts never strand a
    # client session. (Legacy SSE was removed 2026-06 once all clients
    # migrated.)
    if not MCP_BEARER_TOKEN:
        raise SystemExit(
            "MCP_BEARER_TOKEN is not set. Refusing to start an unauthenticated "
            "MCP server. Set it in .secrets.env and restart."
        )
    app = BearerAuthMiddleware(mcp.streamable_http_app(), expected_token=MCP_BEARER_TOKEN)
    uvicorn.run(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
