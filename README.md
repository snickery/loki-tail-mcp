# loki-tail-mcp

An [MCP](https://modelcontextprotocol.io) server for
[Grafana Loki](https://grafana.com/oss/loki/), designed around how LLMs
actually query logs: compact output, hard row caps, and a
**fuzzy container-name rescue** that turns the classic
empty-result-because-wrong-name failure into an auto-corrected retry or
an actionable suggestion list. Built on the
[Python MCP SDK](https://github.com/modelcontextprotocol/python-sdk)
(FastMCP); runs as a local stdio server or a containerized Streamable
HTTP service with bearer auth.

## Tools

| Tool | Notes |
|---|---|
| `loki_tail_container` | "What is service X saying?" — the primary tool. Accepts approximate names: service vocabulary (`vpn`, `proxy`), bare names (`sonarr`), typos. On zero results it distinguishes *valid-but-quiet* from *unknown name*, auto-substitutes a unique fuzzy match (flagged in the output), or suggests candidates. |
| `loki_query_range` | Raw LogQL range query — multi-container correlation (`{container=~"a\|b"} \|= "..."`) and metric queries (`count_over_time(...)`). |
| `loki_query_instant` | Instant query at a point in time (metric queries). |
| `loki_list_containers` | Durable container names (ephemeral CI/batch names hidden). |
| `loki_list_labels` / `loki_list_label_values` | Raw label discovery. |
| `loki_patterns` | Log pattern mining — thousands of lines → ranked recurring templates with counts. Requires the server-side [pattern ingester](https://grafana.com/docs/loki/latest/operations/query-patterns/) (`pattern_ingester.enabled: true`). |
| `loki_log_volume` | Rank containers by log bytes over a window — "which service suddenly got noisy". |
| `loki_detected_fields` | Fields Loki can auto-extract from a stream (name/type/cardinality/parser) — discover `\| logfmt \| status>=500` opportunities before writing LogQL. |

### The name-resolution design

Matching always runs against **live label values**, never a hardcoded
list, so it survives renames. Resolution tries, in order: exact match →
alias vocabulary → substring both ways → typo distance (difflib). A
unique candidate is tailed automatically and flagged; multiple candidates
become a ranked suggestion list. Auto-generated container names
(docker/podman `adjective_noun`, hex-suffixed batch workers) are filtered
out of discovery and suggestions but stay queryable via raw LogQL.

The built-in alias vocabulary covers the common self-hosted stack
(`vpn`→gluetun, `proxy`→traefik, `movies`→radarr, …). Entries whose
targets don't exist in your fleet are inert; extend with your own via
`LOKI_ALIASES`.

## Quick start (stdio)

```jsonc
// e.g. Claude Desktop claude_desktop_config.json / Claude Code .mcp.json
{
  "mcpServers": {
    "loki": {
      "command": "uv",
      "args": ["run", "--project", "/path/to/loki-tail-mcp", "loki-tail-mcp", "--stdio"],
      "env": { "LOKI_URL": "http://your-loki-host:3100" }
    }
  }
}
```

stdio mode has no network surface and skips bearer auth — the client owns
the process.

## HTTP mode (container)

The bundled `Containerfile` builds a Streamable HTTP server at `/mcp`
(stateless — restarts never strand client sessions). HTTP mode **refuses
to start** without `MCP_BEARER_TOKEN`; clients authenticate with
`Authorization: Bearer <token>`.

```bash
podman build -t loki-tail-mcp .   # or: docker build -t loki-tail-mcp .
podman run -d --name loki-tail-mcp -p 8325:8325 \
  -e LOKI_URL=http://your-loki-host:3100 \
  -e MCP_BEARER_TOKEN=some-long-random-token \
  loki-tail-mcp
```

`loki_tail_mcp.healthcheck` does a full HTTP round-trip to `/mcp` (the 401
counts as alive); wire it to your container healthcheck. Terminate TLS at
a reverse proxy — the server itself speaks plain HTTP.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `LOKI_URL` | `http://loki:3100` | Loki base URL. |
| `LOKI_TENANT_ID` | *(empty)* | Sent as `X-Scope-OrgID` for multi-tenant Loki. |
| `LOKI_BASIC_AUTH` | *(empty)* | `user:password` for a basic-auth-fronted Loki (reverse proxy, Grafana Cloud). |
| `LOKI_TIMEOUT` | `30` | Upstream request timeout (s). |
| `LOKI_DEFAULT_LIMIT` / `LOKI_MAX_LIMIT` | `100` / `1000` | Row caps — Loki will happily return millions of rows; an MCP client will happily feed them to an LLM. Neither is what you want. |
| `LOKI_ALIASES` | *(empty)* | Extra vocabulary merged over the built-ins: `term=fragment` or `term=frag\|frag2`, comma-separated (e.g. `cache=redis\|valkey,db=postgres`). |
| `LOKI_EPHEMERAL_PATTERNS` | *(built-ins)* | Comma-separated regexes marking names as ephemeral; replaces the defaults when set. |
| `PORT` | `8325` | HTTP listen port. |
| `MCP_BEARER_TOKEN` | *(empty)* | Required in HTTP mode; server refuses to start without it. Not used in `--stdio` mode. |

## Testing

```bash
# Full suite — mocked HTTP + pure resolution logic, no Loki needed
uv run --extra test pytest tests/ -v
```

## License

[MIT](LICENSE)
