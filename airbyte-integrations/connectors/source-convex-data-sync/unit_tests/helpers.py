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


def obj(fields):
    return {"type": "object", "value": {name: {"fieldType": ft, "optional": opt} for name, (ft, opt) in fields.items()}}


POSTS_VALIDATOR = obj(
    {
        "author": ({"type": "id", "tableName": "users"}, False),
        "body": ({"type": "string"}, False),
    }
)

USER_VALIDATOR = obj(
    {
        "email": ({"type": "string"}, False),
        "name": ({"type": "union", "value": [{"type": "null"}, {"type": "string"}]}, True),
    }
)

RATE_LIMITS_VALIDATOR = obj({"name": ({"type": "string"}, False), "value": ({"type": "number"}, False)})

SCHEMA = {
    "": {"posts": POSTS_VALIDATOR},
    "betterAuth": {"user": USER_VALIDATOR},
    "resend/rateLimiter": {"rateLimits": RATE_LIMITS_VALIDATOR},
}

POSTS_SCHEMA = {"type": "object", "properties": {"_id": {"type": "string"}, "author": {"type": "string"}, "body": {"type": "string"}}}
USER_SCHEMA = {"type": "object", "properties": {"_id": {"type": "string"}, "email": {"type": "string"}, "name": {"type": "string"}}}
RATE_LIMITS_SCHEMA = {"type": "object", "properties": {"_id": {"type": "string"}, "name": {"type": "string"}, "value": {"type": "number"}}}


@pytest.fixture
def inline_config():
    return {"deployment_url": DEPLOYMENT_URL, "access_key": "test_api_key", "schema_json": json.dumps(SCHEMA)}


def configured_stream(component, table, schema, sync_mode=SyncMode.incremental):
    return ConfiguredAirbyteStream(
        stream=AirbyteStream(
            name=table,
            namespace=component or None,
            json_schema=dict(schema),
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
            configured_stream("", "posts", POSTS_SCHEMA),
            configured_stream("betterAuth", "user", USER_SCHEMA),
            configured_stream("resend/rateLimiter", "rateLimits", RATE_LIMITS_SCHEMA),
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


POSTS = (None, "posts")
USER = ("betterAuth", "user")
RATE_LIMITS = ("resend/rateLimiter", "rateLimits")


def ident(record_or_descriptor):
    """(namespace, name) of a record, stream descriptor, or stream."""
    obj = record_or_descriptor
    name = getattr(obj, "stream", None) if isinstance(getattr(obj, "stream", None), str) else getattr(obj, "name", None)
    return (obj.namespace, name)
