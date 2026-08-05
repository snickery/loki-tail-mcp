"""Container-name resolution for the loki_tail_container zero-result rescue.

The dominant failure mode for LLM callers of `loki_tail_container` is passing
service vocabulary ("vpn", "proxy") or a bare service name ("sonarr") instead
of the exact Loki `container` label value ("arr-vpn-gluetun", "arr-sonarr").
The tail tool then returns an empty result with no hint, which a single-shot
caller with a tight tool-call budget cannot recover from. This module turns
that empty result into either an auto-resolved retry or an actionable
suggestion list.

Matching runs against LIVE label values, never a hardcoded name list, so it
survives container renames. The ALIASES map only bridges vocabulary that
shares no substring with its implementation's container name; entries whose
targets don't exist in your fleet are inert. Extend it via the LOKI_ALIASES
env var; override the ephemeral patterns via LOKI_EPHEMERAL_PATTERNS.
"""

from __future__ import annotations

import difflib
import re

from loki_tail_mcp.config import LOKI_ALIASES, LOKI_EPHEMERAL_PATTERNS

# Ephemeral container families that pollute the `container` label space —
# potentially thousands of values in a 24h window. They are real Loki
# streams and stay queryable via LogQL; they are just excluded from discovery
# listings and name-resolution candidate pools. Defaults cover:
#   - docker/podman auto-names (adjective_noun), e.g. from CI-in-container churn
#   - hex-suffixed batch workers (worker-<hash>)
_DEFAULT_EPHEMERAL_PATTERNS = (r"^[a-z]+_[a-z0-9]+$", r"-[0-9a-f]{6,}$")

_EPHEMERAL_RES = tuple(
    re.compile(p)
    for p in (
        [p.strip() for p in LOKI_EPHEMERAL_PATTERNS.split(",") if p.strip()]
        or _DEFAULT_EPHEMERAL_PATTERNS
    )
)


def parse_alias_spec(raw: str) -> dict[str, tuple[str, ...]]:
    """Parse 'cache=redis|valkey,db=postgres' into an alias map."""
    out: dict[str, tuple[str, ...]] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        term, _, frags = pair.partition("=")
        term = term.strip().lower()
        targets = tuple(f.strip().lower() for f in frags.split("|") if f.strip())
        if not term or not targets:
            raise ValueError(
                f"Bad LOKI_ALIASES entry {pair!r} — expected term=fragment"
                " or term=frag|frag2"
            )
        out[term] = targets
    return out


# Service vocabulary → container-name fragments (substring targets against
# live names). Built-in defaults cover the common self-hosted stack; only
# add terms that don't already substring-match the container they mean —
# substring matching handles the rest for free. Entries with no matching
# live container are harmless. LOKI_ALIASES merges over these.
ALIASES: dict[str, tuple[str, ...]] = {
    "vpn": ("gluetun",),
    "wireguard": ("gluetun",),
    "mullvad": ("gluetun",),
    "proxy": ("traefik",),
    "reverse-proxy": ("traefik",),
    "ingress": ("traefik",),
    "ids": ("crowdsec",),
    "firewall": ("crowdsec",),
    "bans": ("crowdsec",),
    "sso": ("authentik",),
    "identity": ("authentik",),
    "ha": ("homeassistant",),
    "hass": ("homeassistant", "hass-mcp"),
    "home-automation": ("homeassistant",),
    "smart-home": ("homeassistant",),
    "usenet": ("sabnzbd",),
    "nzb": ("sabnzbd",),
    "downloads": ("sabnzbd",),
    "downloader": ("sabnzbd",),
    "photos": ("immich",),
    "pictures": ("immich",),
    "documents": ("paperless",),
    "dms": ("paperless",),
    "git": ("forgejo",),
    "ci": ("act-runner",),
    "runner": ("act-runner",),
    "gateway": ("litellm",),
    "llm-gateway": ("litellm",),
    "chat": ("librechat",),
    "logs": ("loki", "alloy"),
    "logging": ("loki", "alloy"),
    "metrics": ("grafana", "victoriametrics"),
    "dashboards": ("grafana",),
    "notifications": ("ntfy",),
    "alerts": ("ntfy",),
    "alerting": ("ntfy",),
    "push": ("ntfy",),
    "monitor": ("uptime-kuma",),
    "monitoring": ("uptime-kuma",),
    "movies": ("radarr",),
    "tv": ("sonarr",),
    "shows": ("sonarr",),
    "series": ("sonarr",),
    "music": ("lidarr",),
    "books": ("readarr", "calibre"),
    "subtitles": ("bazarr", "subgen"),
    "subs": ("bazarr", "subgen"),
    "indexer": ("prowlarr",),
    "indexers": ("prowlarr",),
    "requests": ("seerr",),
    "antivirus": ("clamav",),
    "virus": ("clamav",),
    "malware": ("clamav",),
    "cve": ("trivy",),
    "vuln": ("trivy",),
    "vulnerabilities": ("trivy",),
    "tailnet": ("tailscale",),
}
ALIASES.update(parse_alias_spec(LOKI_ALIASES))

# Substring matching below this length is noise ("ha" would match "chat");
# short vocabulary goes through ALIASES instead.
_MIN_SUBSTRING_LEN = 3

_MAX_SUGGESTIONS = 8


def is_ephemeral(name: str) -> bool:
    """True for auto-named / batch-worker container names."""
    return any(r.search(name) for r in _EPHEMERAL_RES)


def filter_ephemeral(names: list[str]) -> tuple[list[str], int]:
    """Split ephemeral names out of a label-values list.

    Returns (durable_names, hidden_count).
    """
    durable = [n for n in names if not is_ephemeral(n)]
    return durable, len(names) - len(durable)


def resolve_container(
    requested: str, names: list[str]
) -> tuple[str | None, list[str]]:
    """Resolve a requested container name against live label values.

    `names` should already be ephemeral-filtered. Returns (match,
    suggestions): `match` is set only when exactly one candidate survives
    (safe to auto-substitute); otherwise `suggestions` holds up to
    _MAX_SUGGESTIONS candidates ranked best-first (alias hits before
    substring hits before typo-distance hits), possibly empty.
    """
    req = requested.strip().lower()
    if not req or not names:
        return None, []

    by_lower: dict[str, str] = {}
    for n in names:
        by_lower.setdefault(n.lower(), n)

    if req in by_lower:
        return by_lower[req], []

    candidates: list[str] = []

    def add(matched):
        for n in matched:
            if n not in candidates:
                candidates.append(n)

    for frag in ALIASES.get(req, ()):
        add(n for n in names if frag in n.lower())
    if len(req) >= _MIN_SUBSTRING_LEN:
        add(n for n in names if req in n.lower() or n.lower() in req)
    add(
        by_lower[m]
        for m in difflib.get_close_matches(req, list(by_lower), n=5, cutoff=0.75)
    )

    if len(candidates) == 1:
        return candidates[0], []
    return None, candidates[:_MAX_SUGGESTIONS]
