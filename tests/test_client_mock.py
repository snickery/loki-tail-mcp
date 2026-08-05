from unittest.mock import MagicMock, patch

from loki_tail_mcp.loki_client import LokiClient, LokiError
import pytest


def _ok(json_body):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = json_body
    return resp


def _err(status, text):
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    return resp


def test_query_range_builds_params():
    client = LokiClient(base_url="http://loki:3100")
    with patch.object(client._client, "get", return_value=_ok({"data": {}})) as g:
        client.query_range(
            '{container="example-app"}',
            start_ns=1000,
            end_ns=2000,
            limit=50,
            direction="backward",
        )
        args, kwargs = g.call_args
        assert args[0] == "http://loki:3100/loki/api/v1/query_range"
        params = kwargs["params"]
        assert params["query"] == '{container="example-app"}'
        assert params["start"] == "1000"
        assert params["end"] == "2000"
        assert params["limit"] == 50
        assert params["direction"] == "backward"
        assert "step" not in params  # None values stripped


def test_query_range_keeps_step_when_set():
    client = LokiClient()
    with patch.object(client._client, "get", return_value=_ok({"data": {}})) as g:
        client.query_range("{x=\"y\"}", 1, 2, 10, step="30s")
        assert g.call_args.kwargs["params"]["step"] == "30s"


def test_query_instant_hits_query_endpoint():
    client = LokiClient()
    with patch.object(client._client, "get", return_value=_ok({"data": {}})) as g:
        client.query_instant("up", time_ns=1234, limit=10)
        assert g.call_args.args[0].endswith("/loki/api/v1/query")
        assert g.call_args.kwargs["params"]["time"] == "1234"


def test_labels_extracts_data_list():
    client = LokiClient()
    with patch.object(
        client._client,
        "get",
        return_value=_ok({"data": ["container", "job", "host"]}),
    ):
        assert client.labels(0, 1) == ["container", "job", "host"]


def test_label_values_encodes_name_in_path():
    client = LokiClient()
    with patch.object(
        client._client,
        "get",
        return_value=_ok({"data": ["sonarr", "radarr"]}),
    ) as g:
        values = client.label_values("container", 0, 1)
        assert g.call_args.args[0].endswith("/loki/api/v1/label/container/values")
        assert values == ["sonarr", "radarr"]


def test_non_200_raises_loki_error():
    client = LokiClient()
    with patch.object(client._client, "get", return_value=_err(400, "bad query")):
        with pytest.raises(LokiError, match="400"):
            client.query_range("bogus", 0, 1, 10)


def test_patterns_builds_params():
    client = LokiClient(base_url="http://loki:3100")
    with patch.object(client._client, "get", return_value=_ok(
        {"status": "success", "data": [{"pattern": "x <_>", "samples": [[1, 5]]}]}
    )) as g:
        out = client.patterns('{container="a"}', 1000, 2000)
        assert g.call_args[0][0] == "http://loki:3100/loki/api/v1/patterns"
        assert g.call_args[1]["params"]["query"] == '{container="a"}'
        assert out[0]["pattern"] == "x <_>"


def test_index_volume_builds_params():
    client = LokiClient(base_url="http://loki:3100")
    with patch.object(client._client, "get", return_value=_ok(
        {"status": "success", "data": {"resultType": "vector", "result": [
            {"metric": {"container": "a"}, "value": [1, "2048"]}]}}
    )) as g:
        out = client.index_volume('{container=~".+"}', 1000, 2000, limit=5)
        assert g.call_args[0][0] == "http://loki:3100/loki/api/v1/index/volume"
        assert g.call_args[1]["params"]["limit"] == 5
        assert out[0]["metric"]["container"] == "a"


def test_detected_fields_builds_params():
    client = LokiClient(base_url="http://loki:3100")
    with patch.object(client._client, "get", return_value=_ok(
        {"fields": [{"label": "status", "type": "int", "cardinality": 7,
                     "parsers": ["logfmt"]}]}
    )) as g:
        out = client.detected_fields('{container="a"}', 1000, 2000, limit=9)
        assert g.call_args[0][0] == "http://loki:3100/loki/api/v1/detected_fields"
        assert g.call_args[1]["params"]["limit"] == 9
        assert out[0]["label"] == "status"
