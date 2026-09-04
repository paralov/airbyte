#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json

import pytest

from airbyte_cdk.models import (
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)


DEPLOYMENT_URL = "https://murky-swan-635.convex.cloud"
SYNC_URL = f"{DEPLOYMENT_URL}/api/v1/data/sync"
ACTIVE_SYNCS_URL = f"{DEPLOYMENT_URL}/api/v1/data/list_active_syncs"
JSON_SCHEMAS_URL = f"{DEPLOYMENT_URL}/api/json_schemas"

POSTS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "_id": {"type": "string", "$description": "Id(posts)"},
        "_creationTime": {"type": "number"},
        "author": {"type": "string", "$description": "Id(users)"},
        "body": {"type": "string"},
    },
    "required": ["_id", "_creationTime", "author", "body"],
    "additionalProperties": False,
}

USER_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "_id": {"type": "string"},
        "_creationTime": {"type": "number"},
        "email": {"type": "string"},
        "name": {"type": "string"},
    },
    "required": ["_id", "_creationTime", "email"],
    "additionalProperties": False,
}

RATE_LIMITS_SCHEMA = {
    "type": "object",
    "properties": {"_id": {"type": "string"}, "_creationTime": {"type": "number"}, "name": {"type": "string"}, "value": {"type": "number"}},
}

INLINE_SCHEMA = {
    "": {"posts": POSTS_SCHEMA},
    "betterAuth": {"user": USER_SCHEMA},
    "resend/rateLimiter": {"rateLimits": RATE_LIMITS_SCHEMA},
}


@pytest.fixture
def api_config():
    return {"deployment_url": DEPLOYMENT_URL, "access_key": "test_api_key", "schema_source": {"type": "api"}}


@pytest.fixture
def inline_config():
    return {
        "deployment_url": DEPLOYMENT_URL,
        "access_key": "test_api_key",
        "schema_source": {"type": "inline", "schema_json": json.dumps(INLINE_SCHEMA)},
    }


def configured_stream(name, component, table, schema, sync_mode=SyncMode.incremental):
    json_schema = dict(schema)
    json_schema["x-convex-component"] = component
    json_schema["x-convex-table"] = table
    return ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name=name,
            json_schema=json_schema,
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
            source_defined_cursor=True,
            default_cursor_field=["_ts"],
            source_defined_primary_key=[["_id"]],
        ),
        sync_mode=sync_mode,
        destination_sync_mode=DestinationSyncMode.append_dedup,
        cursor_field=["_ts"],
        primary_key=[["_id"]],
    )


@pytest.fixture
def catalog():
    return ConfiguredAirbyteCatalog(
        streams=[
            configured_stream("posts", "", "posts", POSTS_SCHEMA),
            configured_stream("betterAuth__user", "betterAuth", "user", USER_SCHEMA),
            configured_stream("resend__rateLimiter__rateLimits", "resend/rateLimiter", "rateLimits", RATE_LIMITS_SCHEMA),
        ]
    )


def page(values=(), truncates=(), status=None, cursor="c1", has_more=True, sync_id="sync-1"):
    return {
        "status": status or {"type": "snapshotting"},
        "truncates": list(truncates),
        "values": list(values),
        "syncId": sync_id,
        "pagination": {"hasMore": has_more, "nextCursor": cursor},
    }


def value(component, table, doc, ts=1_788_466_011_116_811_508, deleted=False):
    return {"component": component, "table": table, "ts": ts, "deleted": deleted, "value": doc}
