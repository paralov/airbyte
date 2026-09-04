#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging

import pytest
from source_convex_data_sync.source import SourceConvexDataSync, build_selection, parse_inline_schema, stream_name_for

from airbyte_cdk.models import SyncMode
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from unit_tests.helpers import ACTIVE_SYNCS_URL, INLINE_SCHEMA, JSON_SCHEMAS_URL, POSTS_SCHEMA, USER_SCHEMA


logger = logging.getLogger("airbyte")


def test_stream_names():
    assert stream_name_for("", "posts") == "posts"
    assert stream_name_for("betterAuth", "user") == "betterAuth__user"
    assert stream_name_for("resend/emailWorkpool", "payload") == "resend__emailWorkpool__payload"


def test_build_selection_includes_only_requested_tables():
    selection = build_selection([("", "posts"), ("betterAuth", "user"), ("betterAuth", "session")])
    assert selection == {
        "_other": "excluded",
        "": {"_other": "excluded", "posts": {"_other": "included"}},
        "betterAuth": {"_other": "excluded", "user": {"_other": "included"}, "session": {"_other": "included"}},
    }


def test_parse_inline_schema_nested():
    nested = parse_inline_schema(json.dumps(INLINE_SCHEMA))
    assert set(nested) == {("", "posts"), ("betterAuth", "user"), ("resend/rateLimiter", "rateLimits")}


def test_parse_inline_schema_component_with_table_named_type():
    # A table literally called "type" must not make the component map look like a bare JSON Schema.
    parsed = parse_inline_schema(json.dumps({"betterAuth": {"type": USER_SCHEMA, "user": USER_SCHEMA}}))
    assert set(parsed) == {("betterAuth", "type"), ("betterAuth", "user")}


def test_parse_inline_schema_rejects_flat_form_with_hint():
    with pytest.raises(AirbyteTracedException) as err:
        parse_inline_schema(json.dumps({"posts": POSTS_SCHEMA}))
    assert '{"": {"posts": <JSON Schema>}}' in err.value.message


@pytest.mark.parametrize("bad", ["not json", "[]", json.dumps({"x": 1}), json.dumps({"": {"posts": "nope"}})])
def test_parse_inline_schema_rejects_bad_input(bad):
    with pytest.raises(AirbyteTracedException):
        parse_inline_schema(bad)


def test_check_connection_ok(requests_mock, inline_config):
    requests_mock.get(ACTIVE_SYNCS_URL, json={"syncs": [], "pagination": {"hasMore": False}})
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert ok and error is None
    assert requests_mock.last_request.headers["Authorization"] == "Convex test_api_key"
    assert requests_mock.last_request.headers["Convex-Client"].startswith("airbyte-data-sync-")


def test_check_connection_bad_key(requests_mock, inline_config):
    requests_mock.get(ACTIVE_SYNCS_URL, status_code=401, json={"code": "Unauthenticated", "message": "bad key"})
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert not ok
    assert "Unauthenticated" in error and "bad key" in error


def test_check_connection_api_schema_failure(requests_mock, api_config):
    requests_mock.get(ACTIVE_SYNCS_URL, json={"syncs": [], "pagination": {"hasMore": False}})
    requests_mock.get(JSON_SCHEMAS_URL, status_code=400, json={"code": "Error code", "message": "Error message"})
    ok, error = SourceConvexDataSync().check_connection(logger, api_config)
    assert not ok
    assert "Error code" in error


@pytest.mark.parametrize("response", [{"json": "Unauthorized"}, {"json": ["nope"]}, {"text": "<html>gateway</html>"}])
def test_check_connection_reports_non_object_error_bodies(requests_mock, inline_config, response):
    requests_mock.get(ACTIVE_SYNCS_URL, status_code=401, **response)
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert not ok
    assert "401" in error


def test_check_connection_reports_non_json_success_body(requests_mock, inline_config):
    requests_mock.get(ACTIVE_SYNCS_URL, text="<html>login</html>")
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert not ok
    assert "InvalidJSON" in error


def test_streams_from_inline_schema(inline_config):
    streams = SourceConvexDataSync().streams(inline_config)
    by_name = {s.name: s for s in streams}
    assert set(by_name) == {"posts", "betterAuth__user", "resend__rateLimiter__rateLimits"}
    user = by_name["betterAuth__user"]
    assert user.primary_key == "_id"
    assert user.cursor_field == "_ts"
    assert user.supports_incremental and user.source_defined_cursor
    schema = user.get_json_schema()
    assert schema["x-convex-component"] == "betterAuth"
    assert schema["x-convex-table"] == "user"
    assert schema["additionalProperties"] is True
    for field in ("_ts", "_deleted", "_component", "_table", "_ab_cdc_lsn", "_ab_cdc_updated_at", "_ab_cdc_deleted_at"):
        assert field in schema["properties"]
    assert schema["properties"]["email"] == {"type": "string"}


def test_streams_from_api(requests_mock, api_config):
    requests_mock.get(JSON_SCHEMAS_URL, json={"": {"posts": POSTS_SCHEMA}, "betterAuth": {"user": USER_SCHEMA}})
    streams = SourceConvexDataSync().streams(api_config)
    assert [s.name for s in streams] == ["posts", "betterAuth__user"]
    assert [(s.component, s.table) for s in streams] == [("", "posts"), ("betterAuth", "user")]
    # Schemas are grouped by component and describe the export encoding the data sync endpoint uses.
    assert requests_mock.last_request.qs == {"deltaschema": ["true"], "format": ["export_json"], "bycomponent": ["true"]}


COLLIDING_CONFIG = {
    "deployment_url": "https://murky-swan-635.convex.cloud",
    "access_key": "k",
    "schema_source": {"type": "inline", "schema_json": json.dumps({"": {"audit__log": POSTS_SCHEMA}, "audit": {"log": USER_SCHEMA}})},
}


def test_streams_rejects_colliding_stream_names():
    with pytest.raises(AirbyteTracedException) as err:
        SourceConvexDataSync().streams(COLLIDING_CONFIG)
    assert "audit__log" in err.value.message


def test_check_connection_reports_colliding_stream_names(requests_mock):
    requests_mock.get(ACTIVE_SYNCS_URL, json={"syncs": [], "pagination": {"hasMore": False}})
    ok, error = SourceConvexDataSync().check_connection(logger, COLLIDING_CONFIG)
    assert not ok and "audit__log" in error


def test_check_connection_rejects_malformed_deployment_url(inline_config, monkeypatch):
    sleeps = []
    monkeypatch.setattr("source_convex_data_sync.source.time.sleep", sleeps.append)
    inline_config["deployment_url"] = "murky-swan-635.convex.cloud"
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert not ok and "No scheme supplied" in error
    assert sleeps == []


def test_discover_catalog(inline_config):
    catalog = SourceConvexDataSync().discover(logger, inline_config)
    stream = next(s for s in catalog.streams if s.name == "posts")
    assert stream.supported_sync_modes == [SyncMode.full_refresh, SyncMode.incremental]
    assert stream.default_cursor_field == ["_ts"]
    assert stream.source_defined_primary_key == [["_id"]]
