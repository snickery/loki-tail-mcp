import pytest

from loki_tail_mcp.resolve import (
    filter_ephemeral,
    is_ephemeral,
    parse_alias_spec,
    resolve_container,
)

# Representative slice of live label values on a typical fleet.
LIVE = [
    "act-runner",
    "alloy",
    "arr-bazarr",
    "arr-prowlarr",
    "arr-radarr",
    "arr-sabnzbd",
    "arr-sonarr",
    "arr-subgen",
    "arr-vpn-gluetun",
    "authentik-server",
    "authentik-worker",
    "clamav",
    "crowdsec",
    "forgejo",
    "gluetun-webui",
    "files-app",
    "hass-web",
    "homeassistant",
    "immich-server",
    "librechat-server",
    "litellm",
    "loki",
    "loki-grafana",
    "loki-mcp",
    "loki-victoriametrics",
    "notes-app",
    "ntfy",
    "paperless",
    "plex",
    "seerr",
    "tailscale-traefik",
    "trivy-server",
    "uptime-kuma",
]


# --- ephemeral detection --------------------------------------------------


def test_podman_auto_names_are_ephemeral():
    for name in ("adoring_neumann", "awesome_curie", "angry_tesla2"):
        assert is_ephemeral(name)


def test_hex_suffixed_batch_workers_are_ephemeral():
    for name in ("quality-ffmpeg-0030daf8", "transcode-ffmpeg-fee7dd5d"):
        assert is_ephemeral(name)


def test_durable_names_are_not_ephemeral():
    for name in LIVE:
        assert not is_ephemeral(name), name


def test_filter_ephemeral_counts_hidden():
    mixed = LIVE + ["adoring_neumann", "transcode-ffmpeg-fee7dd5d"]
    durable, hidden = filter_ephemeral(mixed)
    assert durable == LIVE
    assert hidden == 2


# --- resolution -----------------------------------------------------------


def test_exact_match_case_insensitive():
    match, suggestions = resolve_container("Plex", LIVE)
    assert match == "plex"
    assert suggestions == []


def test_bare_service_name_resolves_to_prefixed_container():
    match, suggestions = resolve_container("sonarr", LIVE)
    assert match == "arr-sonarr"
    assert suggestions == []


def test_alias_with_multiple_candidates_suggests():
    match, suggestions = resolve_container("vpn", LIVE)
    assert match is None
    assert set(suggestions) == {"arr-vpn-gluetun", "gluetun-webui"}


def test_alias_unique_candidate_resolves():
    match, _ = resolve_container("movies", LIVE)
    assert match == "arr-radarr"


def test_short_alias_does_not_substring_spray():
    # "ha" must go through the alias map, not substring-match librechat etc.
    match, _ = resolve_container("ha", LIVE)
    assert match == "homeassistant"


def test_substring_multiple_candidates_suggests():
    match, suggestions = resolve_container("authentik", LIVE)
    assert match is None
    assert suggestions == ["authentik-server", "authentik-worker"]


def test_typo_resolves_via_close_match():
    match, suggestions = resolve_container("crowdsek", LIVE)
    assert match == "crowdsec"


def test_garbage_returns_nothing():
    match, suggestions = resolve_container("zzqx-not-a-thing", LIVE)
    assert match is None
    assert suggestions == []


def test_empty_inputs():
    assert resolve_container("", LIVE) == (None, [])
    assert resolve_container("plex", []) == (None, [])


def test_alias_hits_rank_before_substring_hits():
    # "logs" → alias (loki, alloy) plus no substring hits; both suggested.
    match, suggestions = resolve_container("logs", LIVE)
    assert match is None
    assert suggestions[0] == "loki"
    assert "alloy" in suggestions


# --- LOKI_ALIASES env parsing ---------------------------------------------


class TestParseAliasSpec:
    def test_empty(self):
        assert parse_alias_spec("") == {}

    def test_single(self):
        assert parse_alias_spec("cache=redis") == {"cache": ("redis",)}

    def test_multi_target_and_entries(self):
        assert parse_alias_spec("cache=redis|valkey, db=postgres") == {
            "cache": ("redis", "valkey"),
            "db": ("postgres",),
        }

    def test_case_normalized(self):
        assert parse_alias_spec("Cache=Redis") == {"cache": ("redis",)}

    def test_bad_entry_raises(self):
        with pytest.raises(ValueError, match="LOKI_ALIASES"):
            parse_alias_spec("cache")
        with pytest.raises(ValueError, match="LOKI_ALIASES"):
            parse_alias_spec("=redis")
