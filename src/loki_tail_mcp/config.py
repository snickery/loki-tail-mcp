import os

# --- Loki backend --------------------------------------------------------
# Default assumes a container network where DNS resolves `loki`; point
# LOKI_URL wherever your Loki lives.
LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100")
LOKI_TIMEOUT = float(os.environ.get("LOKI_TIMEOUT", "30"))

# Optional upstream auth: X-Scope-OrgID for multi-tenant Loki, and/or
# "user:password" for a basic-auth-fronted instance (reverse proxy,
# Grafana Cloud).
LOKI_TENANT_ID = os.environ.get("LOKI_TENANT_ID", "")
LOKI_BASIC_AUTH = os.environ.get("LOKI_BASIC_AUTH", "")

# --- Name-resolution vocabulary ------------------------------------------
# Extra alias entries merged over the built-in defaults in resolve.py:
# "term=fragment" or "term=frag|frag2", comma-separated
# (e.g. "cache=redis|valkey,db=postgres").
LOKI_ALIASES = os.environ.get("LOKI_ALIASES", "")
# Comma-separated regexes marking container names as ephemeral (hidden
# from discovery/suggestions). REPLACES the built-in defaults when set.
LOKI_EPHEMERAL_PATTERNS = os.environ.get("LOKI_EPHEMERAL_PATTERNS", "")

# Safety caps — Loki will happily return millions of rows; an MCP client
# will happily feed them to an LLM. Neither is what you want.
DEFAULT_LIMIT = int(os.environ.get("LOKI_DEFAULT_LIMIT", "100"))
MAX_LIMIT = int(os.environ.get("LOKI_MAX_LIMIT", "1000"))

# --- Server --------------------------------------------------------------
PORT = int(os.environ.get("PORT", "8325"))
MCP_BEARER_TOKEN = os.environ.get("MCP_BEARER_TOKEN", "")
