#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging

import pytest
from source_convex_data_sync.source import (
    SourceConvexDataSync,
    build_selection,
    parse_schema_json,
    validator_to_json_schema,
)

from airbyte_cdk.models import Status, SyncMode
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from unit_tests.helpers import ACTIVE_SYNCS_URL, POSTS_VALIDATOR, SCHEMA, USER_VALIDATOR, obj


logger = logging.getLogger("airbyte")


def test_streams_are_named_by_table_with_component_namespace(inline_config):
    streams = SourceConvexDataSync().streams(inline_config)
    assert [(s.namespace, s.name) for s in streams] == [(None, "posts"), ("betterAuth", "user"), ("resend/rateLimiter", "rateLimits")]


def test_build_selection_includes_only_requested_tables():
    selection = build_selection([("", "posts"), ("betterAuth", "user"), ("betterAuth", "session")])
    assert selection == {
        "_other": "excluded",
        "": {"_other": "excluded", "posts": {"_other": "included"}},
        "betterAuth": {"_other": "excluded", "user": {"_other": "included"}, "session": {"_other": "included"}},
    }


def test_parse_schema_json_nested():
    parsed = parse_schema_json(json.dumps(SCHEMA))
    assert set(parsed) == {("", "posts"), ("betterAuth", "user"), ("resend/rateLimiter", "rateLimits")}


def test_parse_schema_json_component_with_table_named_type():
    # A table literally called "type" must not make the component map look like a bare validator.
    parsed = parse_schema_json(json.dumps({"betterAuth": {"type": USER_VALIDATOR, "user": USER_VALIDATOR}}))
    assert set(parsed) == {("betterAuth", "type"), ("betterAuth", "user")}


def test_parse_schema_json_null_marks_schemaless_table():
    parsed = parse_schema_json(json.dumps({"": {"scratch": None}}))
    assert parsed == {("", "scratch"): None}


@pytest.mark.parametrize("validator", [POSTS_VALIDATOR, {"type": "string"}, {"type": "id", "tableName": "users"}])
def test_parse_schema_json_rejects_flat_form_with_hint(validator):
    with pytest.raises(AirbyteTracedException) as err:
        parse_schema_json(json.dumps({"posts": validator}))
    assert '{"": {"posts": <validator>}}' in err.value.message


@pytest.mark.parametrize("bad", ["not json", "[]", json.dumps({"x": 1}), json.dumps({"": {"posts": "nope"}})])
def test_parse_schema_json_rejects_bad_input(bad):
    with pytest.raises(AirbyteTracedException):
        parse_schema_json(bad)


@pytest.mark.parametrize(
    "schema, needle",
    [
        ({"": {"user-profiles": None}}, "'user-profiles' is not a valid Convex table name"),
        ({"": {"_private": None}}, "'_private' is not a valid Convex table name"),
        ({"": {"x" * 65: None}}, "is not a valid Convex table name"),
        ({"better-auth": {"user": None}}, "'better-auth' is not a valid Convex component path"),
        ({"resend/": {"user": None}}, "'resend/' is not a valid Convex component path"),
        # The identifier grammar allows it, but it is the reserved default key at every level of a selection.
        ({"_other": {"user": None}}, "'_other' is reserved"),
        ({"resend/_other": {"user": None}}, "'_other' is reserved"),
    ],
)
def test_parse_schema_json_rejects_names_convex_would_refuse(schema, needle):
    with pytest.raises(AirbyteTracedException) as err:
        parse_schema_json(json.dumps(schema))
    assert needle in err.value.message
    assert err.value.failure_type.value == "config_error"


def test_parse_schema_json_accepts_identifiers_with_underscores():
    parsed = parse_schema_json(json.dumps({"_internal/_sub": {"audit_log2": None}}))
    assert parsed == {("_internal/_sub", "audit_log2"): None}


def test_validator_to_json_schema_covers_every_validator_kind():
    validator = obj(
        {
            "s": ({"type": "string"}, False),
            "n": ({"type": "number"}, False),
            "b": ({"type": "boolean"}, True),
            "big": ({"type": "bigint"}, False),
            "committed": ({"type": "commitTs"}, False),
            "bytes": ({"type": "bytes"}, False),
            "nil": ({"type": "null"}, False),
            "anything": ({"type": "any"}, False),
            "lit": ({"type": "literal", "value": "open"}, False),
            "ref": ({"type": "id", "tableName": "users"}, False),
            "list": ({"type": "array", "value": {"type": "string"}}, False),
            "map": ({"type": "record", "keys": {"type": "string"}, "values": {"fieldType": {"type": "number"}, "optional": False}}, False),
            "either": ({"type": "union", "value": [{"type": "string"}, {"type": "null"}]}, False),
            "nested": (obj({"x": ({"type": "number"}, False)}), False),
        }
    )
    schema = validator_to_json_schema(validator)
    p = schema["properties"]
    assert p["s"] == {"type": "string"}
    assert p["n"]["anyOf"][0] == {"type": "number"}
    assert p["b"] == {"type": "boolean"} and "b" not in schema["required"]
    # The data sync endpoint's export JSON ships int64 as a plain number; only bytes and non-finite floats are wrapped.
    assert p["big"] == {"type": "integer"}
    assert p["committed"] == {"type": "integer"}
    assert p["bytes"]["properties"] == {"$bytes": {"type": "string"}}
    assert p["nil"] == {"type": "null"}
    assert p["anything"] == {}
    assert p["lit"] == {"type": "string", "enum": ["open"]}
    assert p["ref"] == {"type": "string", "$description": "Id(users)"}
    assert p["list"] == {"type": "array", "items": {"type": "string"}}
    assert p["map"] == {"type": "object", "additionalProperties": True}
    assert p["either"] == {"anyOf": [{"type": "string"}, {"type": "null"}]}
    assert p["nested"]["properties"]["x"]["anyOf"][0] == {"type": "number"}
    # Airbyte requires every additionalProperties in a stream schema to be true, so nested objects never set it to false.
    assert "additionalProperties" not in schema
    assert "additionalProperties" not in p["nested"]


def test_validator_to_json_schema_rejects_unknown_kind():
    with pytest.raises(AirbyteTracedException):
        validator_to_json_schema({"type": "tuple"})


def test_validator_to_json_schema_decodes_int64_literals():
    # ``v.literal(5n)`` serialises as base64 little-endian bytes, while the data sync export ships a plain number.
    assert validator_to_json_schema({"type": "literal", "value": {"$integer": "BQAAAAAAAAA="}}) == {"type": "integer", "enum": [5]}
    assert validator_to_json_schema({"type": "literal", "value": {"$integer": "//////////8="}}) == {"type": "integer", "enum": [-1]}


@pytest.mark.parametrize(
    "validator",
    [
        {"type": "object", "value": {"body": "string"}},
        {"type": "object", "value": [{"a": 1}]},
        {"type": "array", "value": "string"},
        {"type": "union", "value": [1]},
        {"type": "union", "value": []},
        {"type": "union"},
        {"type": "literal", "value": {"$bytes": "AA=="}},
        {"type": "literal", "value": {"$integer": "not base64"}},
        {"type": "literal", "value": {"$integer": "AA=="}},
    ],
)
def test_validator_to_json_schema_rejects_malformed_nested_validators_as_config_errors(validator):
    with pytest.raises(AirbyteTracedException) as err:
        validator_to_json_schema(validator)
    assert err.value.failure_type.value == "config_error"
    assert "Invalid Convex validator" in err.value.message


def test_check_reports_a_malformed_validator_as_a_failed_status(inline_config):
    # A raw AttributeError here would leave the entrypoint with a stack trace instead of a CONNECTION_STATUS message.
    inline_config["schema_json"] = json.dumps({"": {"posts": {"type": "object", "value": {"body": "string"}}}})
    status = SourceConvexDataSync().check(logger, inline_config)
    assert status.status == Status.FAILED
    assert "Invalid Convex validator" in status.message


def test_check_connection_falls_back_to_backoff_on_a_non_finite_retry_after(requests_mock, inline_config, monkeypatch):
    sleeps = []
    monkeypatch.setattr("source_convex_data_sync.source.time.sleep", sleeps.append)
    requests_mock.get(ACTIVE_SYNCS_URL, status_code=503, text="down", headers={"Retry-After": "nan"})
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert not ok and "503" in error
    assert sleeps == [2]


def test_check_connection_ok(requests_mock, inline_config):
    requests_mock.get(ACTIVE_SYNCS_URL, json={"syncs": [], "pagination": {"hasMore": False}})
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert ok and error is None
    assert requests_mock.last_request.headers["Authorization"] == "Convex test_api_key"
    # convex-backend only attributes syncs to Airbyte for the "airbyte-export" client name.
    assert requests_mock.last_request.headers["Convex-Client"].startswith("airbyte-export-")


def test_check_connection_bad_key(requests_mock, inline_config):
    requests_mock.get(ACTIVE_SYNCS_URL, status_code=401, json={"code": "Unauthenticated", "message": "bad key"})
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert not ok
    assert "Unauthenticated" in error and "bad key" in error


def test_check_reports_the_failure_message_unquoted(requests_mock, inline_config):
    # AbstractSource.check would wrap the reason in repr(), showing the user a quoted, escaped string.
    requests_mock.get(ACTIVE_SYNCS_URL, status_code=401, json={"code": "Unauthenticated", "message": "bad 'key'"})
    status = SourceConvexDataSync().check(logger, inline_config)
    assert status.status == Status.FAILED
    assert status.message == "Listing active data syncs: 401: Unauthenticated: bad 'key'"
    requests_mock.get(ACTIVE_SYNCS_URL, json={"syncs": [], "pagination": {"hasMore": False}})
    assert SourceConvexDataSync().check(logger, inline_config).status == Status.SUCCEEDED


def test_check_connection_gives_up_quickly_on_outages(requests_mock, inline_config, monkeypatch):
    sleeps = []
    monkeypatch.setattr("source_convex_data_sync.source.time.sleep", sleeps.append)
    requests_mock.get(ACTIVE_SYNCS_URL, status_code=503, text="down", headers={"Retry-After": "60"})
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert not ok and "503" in error
    # A connection check retries once instead of sitting through the sync path's full backoff schedule.
    assert len(requests_mock.request_history) == 2
    assert sleeps == [60.0]


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
    by_key = {(s.namespace, s.name): s for s in streams}
    user = by_key[("betterAuth", "user")]
    assert user.primary_key == "_id"
    assert user.cursor_field == "_ts"
    assert user.supports_incremental and user.source_defined_cursor
    schema = user.get_json_schema()
    assert schema["additionalProperties"] is True
    for field in ("_ts", "_deleted", "_component", "_table", "_ab_cdc_lsn", "_ab_cdc_updated_at", "_ab_cdc_deleted_at"):
        assert field in schema["properties"]
    assert schema["properties"]["email"] == {"type": "string"}
    assert schema["properties"]["name"] == {"anyOf": [{"type": "null"}, {"type": "string"}]}


def test_streams_schemaless_table_is_permissive():
    config = {
        "deployment_url": "https://murky-swan-635.convex.cloud",
        "access_key": "k",
        "schema_json": json.dumps({"": {"scratch": None}}),
    }
    (stream,) = SourceConvexDataSync().streams(config)
    schema = stream.get_json_schema()
    assert stream.name == "scratch"
    assert schema["additionalProperties"] is True
    assert set(schema["properties"]) == {
        "_id",
        "_creationTime",
        "_ts",
        "_deleted",
        "_component",
        "_table",
        "_ab_cdc_lsn",
        "_ab_cdc_updated_at",
        "_ab_cdc_deleted_at",
    }


def test_same_table_name_in_two_components_yields_two_streams():
    config = {
        "deployment_url": "https://murky-swan-635.convex.cloud",
        "access_key": "k",
        "schema_json": json.dumps({"rateLimiter": {"rateLimits": POSTS_VALIDATOR}, "resend/rateLimiter": {"rateLimits": USER_VALIDATOR}}),
    }
    streams = SourceConvexDataSync().streams(config)
    assert [(s.namespace, s.name) for s in streams] == [("rateLimiter", "rateLimits"), ("resend/rateLimiter", "rateLimits")]
    catalog = SourceConvexDataSync().discover(logger, config)
    assert [(s.namespace, s.name) for s in catalog.streams] == [("rateLimiter", "rateLimits"), ("resend/rateLimiter", "rateLimits")]


def test_check_connection_rejects_malformed_deployment_url(inline_config):
    inline_config["deployment_url"] = "murky-swan-635.convex.cloud"
    ok, error = SourceConvexDataSync().check_connection(logger, inline_config)
    assert not ok and "InvalidDeploymentUrl" in error


def test_discover_catalog(inline_config):
    catalog = SourceConvexDataSync().discover(logger, inline_config)
    stream = next(s for s in catalog.streams if s.name == "posts")
    assert stream.supported_sync_modes == [SyncMode.full_refresh, SyncMode.incremental]
    assert stream.default_cursor_field == ["_ts"]
    assert stream.source_defined_primary_key == [["_id"]]
