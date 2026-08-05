from loki_tail_mcp.loki_client import format_streams


def _streams_response(streams):
    return {"status": "success", "data": {"resultType": "streams", "result": streams}}


def test_format_streams_empty():
    assert format_streams(_streams_response([])) == "(no results)"


def test_format_streams_single_line():
    resp = _streams_response(
        [
            {
                "stream": {"container": "example-app", "level": "info"},
                "values": [["1735689600000000000", "hello world"]],
            }
        ]
    )
    out = format_streams(resp)
    assert "hello world" in out
    assert "container=example-app" in out
    assert "level=info" in out
    assert "2025-01-01T00:00:00.000+00:00" in out


def test_format_streams_sorts_newest_first():
    resp = _streams_response(
        [
            {
                "stream": {"container": "a"},
                "values": [
                    ["1735689600000000000", "older"],
                    ["1735689700000000000", "newer"],
                ],
            }
        ]
    )
    out = format_streams(resp).splitlines()
    assert "newer" in out[0]
    assert "older" in out[1]


def test_format_streams_merges_multiple_streams_sorted():
    resp = _streams_response(
        [
            {
                "stream": {"container": "a"},
                "values": [["1735689600000000000", "from-a"]],
            },
            {
                "stream": {"container": "b"},
                "values": [["1735689700000000000", "from-b"]],
            },
        ]
    )
    lines = format_streams(resp).splitlines()
    assert "from-b" in lines[0]  # newer first
    assert "from-a" in lines[1]


def test_format_streams_filters_labels():
    resp = _streams_response(
        [
            {
                "stream": {"container": "x", "irrelevant": "noise", "level": "error"},
                "values": [["1735689600000000000", "msg"]],
            }
        ]
    )
    out = format_streams(resp)
    assert "container=x" in out
    assert "level=error" in out
    assert "irrelevant" not in out


def test_format_matrix_response():
    resp = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"container": "traefik"},
                    "values": [[1735689600, "0.5"], [1735689660, "0.75"]],
                }
            ],
        },
    }
    out = format_streams(resp)
    assert "container=traefik" in out
    assert "1735689600:0.5" in out
    assert "1735689660:0.75" in out
