"""Tool-level tests for the loki_tail_container zero-result rescue.

server._threaded_tool returns the original sync function from @mcp.tool, so
the tools are directly callable here; the shared LokiClient is swapped for a
mock per-test.
"""

from unittest.mock import MagicMock, patch

from loki_tail_mcp import server


def _streams_response(lines):
    return {
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {"container": "x"},
                    "values": [[str(1_700_000_000_000_000_000 + i), line] for i, line in enumerate(lines)],
                }
            ]
            if lines
            else [],
        }
    }


LIVE = [
    "arr-sonarr",
    "arr-vpn-gluetun",
    "gluetun-webui",
    "example-app",
    "plex",
    "adoring_neumann",
    "transcode-ffmpeg-fee7dd5d",
]


def test_hit_passes_through_without_label_lookup():
    mock = MagicMock()
    mock.query_range.return_value = _streams_response(["all good"])
    with patch.object(server, "loki", mock):
        out = server.loki_tail_container("plex")
    assert "all good" in out
    mock.label_values.assert_not_called()


def test_valid_but_quiet_container_reports_window():
    mock = MagicMock()
    mock.query_range.return_value = _streams_response([])
    mock.label_values.return_value = LIVE
    with patch.object(server, "loki", mock):
        out = server.loki_tail_container("plex", minutes=15)
    assert "has logged within 24h" in out
    assert "15m" in out
    mock.query_range.assert_called_once()


def test_unique_resolution_requeries_and_flags():
    mock = MagicMock()
    empty = _streams_response([])
    hit = _streams_response(["episode grabbed"])
    mock.query_range.side_effect = [empty, hit]
    mock.label_values.return_value = LIVE
    with patch.object(server, "loki", mock):
        out = server.loki_tail_container("sonarr")
    assert out.startswith("[no container 'sonarr' — resolved to 'arr-sonarr']")
    assert "episode grabbed" in out
    resolved_query = mock.query_range.call_args_list[1].args[0]
    assert 'container="arr-sonarr"' in resolved_query


def test_resolved_container_also_quiet_says_so():
    mock = MagicMock()
    mock.query_range.side_effect = [_streams_response([]), _streams_response([])]
    mock.label_values.return_value = LIVE
    with patch.object(server, "loki", mock):
        out = server.loki_tail_container("sonarr")
    assert "resolved to 'arr-sonarr'" in out
    assert "no lines" in out


def test_ambiguous_name_returns_suggestions():
    mock = MagicMock()
    mock.query_range.return_value = _streams_response([])
    mock.label_values.return_value = LIVE
    with patch.object(server, "loki", mock):
        out = server.loki_tail_container("vpn")
    assert "arr-vpn-gluetun" in out
    assert "gluetun-webui" in out
    mock.query_range.assert_called_once()  # no blind re-query


def test_unknown_name_lists_durable_containers_only():
    mock = MagicMock()
    mock.query_range.return_value = _streams_response([])
    mock.label_values.return_value = LIVE
    with patch.object(server, "loki", mock):
        out = server.loki_tail_container("zzqx-not-a-thing")
    assert "plex" in out
    assert "adoring_neumann" not in out
    assert "transcode-ffmpeg" not in out


def test_filter_mentioned_in_quiet_message():
    mock = MagicMock()
    mock.query_range.return_value = _streams_response([])
    mock.label_values.return_value = LIVE
    with patch.object(server, "loki", mock):
        out = server.loki_tail_container("plex", filter="ERROR")
    assert "matching the filter" in out
    assert "drop the `filter`" in out


def test_list_containers_hides_ephemeral_with_count():
    mock = MagicMock()
    mock.label_values.return_value = LIVE
    with patch.object(server, "loki", mock):
        out = server.loki_list_containers()
    assert "plex" in out
    assert "adoring_neumann" not in out
    assert "(2 ephemeral auto-named/batch containers hidden)" in out


# --- new discovery tools (patterns / volume / detected_fields) -------------


class TestPatternsTool:
    def test_ranked_output(self, monkeypatch):
        from loki_tail_mcp import server

        monkeypatch.setattr(server.loki, "patterns", lambda q, s, e: [
            {"pattern": "rare <_>", "level": "info", "samples": [[1, 2]]},
            {"pattern": "common <_>", "level": "error", "samples": [[1, 50], [2, 9]]},
        ])
        out = server.loki_patterns("example-app")
        first_line = out.splitlines()[2]
        assert "common" in first_line and "59x" in first_line
        assert "[error]" in first_line

    def test_empty_hints_at_ingester(self, monkeypatch):
        from loki_tail_mcp import server

        monkeypatch.setattr(server.loki, "patterns", lambda q, s, e: [])
        out = server.loki_patterns("example-app")
        assert "pattern ingester" in out

    def test_name_escaped_into_selector(self, monkeypatch):
        from loki_tail_mcp import server

        seen = {}
        monkeypatch.setattr(
            server.loki, "patterns",
            lambda q, s, e: seen.setdefault("q", q) and [],
        )
        server.loki_patterns('x"} or {y')
        assert seen["q"] == '{container="x\\"} or {y"}'


class TestVolumeTool:
    def test_fleet_ranking(self, monkeypatch):
        from loki_tail_mcp import server

        monkeypatch.setattr(server.loki, "index_volume", lambda q, s, e, limit: [
            {"metric": {"container": "noisy"}, "value": [1, "2097152"]},
            {"metric": {"container": "quiet"}, "value": [1, "512"]},
        ])
        out = server.loki_log_volume()
        assert "2.0 MiB" in out and "noisy" in out
        assert "512 B" in out

    def test_scoped_selector(self, monkeypatch):
        from loki_tail_mcp import server

        seen = {}
        monkeypatch.setattr(
            server.loki, "index_volume",
            lambda q, s, e, limit: seen.setdefault("q", q) and [],
        )
        server.loki_log_volume(container="traefik")
        assert seen["q"] == '{container="traefik"}'


class TestDetectedFieldsTool:
    def test_field_table(self, monkeypatch):
        from loki_tail_mcp import server

        monkeypatch.setattr(server.loki, "detected_fields", lambda q, s, e, limit: [
            {"label": "status", "type": "int", "cardinality": 7, "parsers": ["logfmt"]},
            {"label": "detected_level", "type": "string", "cardinality": 2, "parsers": None},
        ])
        out = server.loki_detected_fields("example-app")
        assert "status" in out and "logfmt" in out
        assert "index/metadata" in out
