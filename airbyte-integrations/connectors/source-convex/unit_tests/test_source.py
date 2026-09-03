#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging

import pytest
from source_convex.source import SourceConvex, build_selection, parse_inline_schema, stream_name_for

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
        "_other": "excl",
        "": {"_other": "excl", "posts": {"_other": "incl"}},
        "betterAuth": {"_other": "excl", "user": {"_other": "incl"}, "session": {"_other": "incl"}},
    }


def test_parse_inline_schema_nested_and_flat():
    nested = parse_inline_schema(json.dumps(INLINE_SCHEMA))
    assert set(nested) == {("", "posts"), ("betterAuth", "user"), ("resend/rateLimiter", "rateLimits")}
    flat = parse_inline_schema(json.dumps({"posts": POSTS_SCHEMA, "users": USER_SCHEMA}))
    assert set(flat) == {("", "posts"), ("", "users")}


@pytest.mark.parametrize("bad", ["not json", "[]", json.dumps({"x": 1})])
def test_parse_inline_schema_rejects_bad_input(bad):
    with pytest.raises(AirbyteTracedException):
        parse_inline_schema(bad)


def test_check_connection_ok(requests_mock, inline_config):
    requests_mock.get(ACTIVE_SYNCS_URL, json={"syncs": [], "pagination": {"hasMore": False}})
    ok, error = SourceConvex().check_connection(logger, inline_config)
    assert ok and error is None
    assert requests_mock.last_request.headers["Authorization"] == "Convex test_api_key"
    assert requests_mock.last_request.headers["Convex-Client"].startswith("airbyte-export-")


def test_check_connection_bad_key(requests_mock, inline_config):
    requests_mock.get(ACTIVE_SYNCS_URL, status_code=401, json={"code": "Unauthenticated", "message": "bad key"})
    ok, error = SourceConvex().check_connection(logger, inline_config)
    assert not ok
    assert "Unauthenticated" in error and "bad key" in error


def test_check_connection_api_schema_failure(requests_mock, api_config):
    requests_mock.get(ACTIVE_SYNCS_URL, json={"syncs": [], "pagination": {"hasMore": False}})
    requests_mock.get(JSON_SCHEMAS_URL, status_code=400, json={"code": "Error code", "message": "Error message"})
    ok, error = SourceConvex().check_connection(logger, api_config)
    assert not ok
    assert "Error code" in error


def test_streams_from_inline_schema(inline_config):
    streams = SourceConvex().streams(inline_config)
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
    requests_mock.get(JSON_SCHEMAS_URL, json={"posts": POSTS_SCHEMA, "users": USER_SCHEMA})
    streams = SourceConvex().streams(api_config)
    assert [s.name for s in streams] == ["posts", "users"]
    assert all(s.get_json_schema()["x-convex-component"] == "" for s in streams)


def test_discover_catalog(inline_config):
    catalog = SourceConvex().discover(logger, inline_config)
    stream = next(s for s in catalog.streams if s.name == "posts")
    assert stream.supported_sync_modes == [SyncMode.full_refresh, SyncMode.incremental]
    assert stream.default_cursor_field == ["_ts"]
    assert stream.source_defined_primary_key == [["_id"]]
