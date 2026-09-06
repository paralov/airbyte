#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import json
import logging
import os
from pathlib import Path

import pytest
from jsonschema import Draft7Validator
from source_convex_data_sync.source import SourceConvexDataSync

from airbyte_cdk.models import ConfiguredAirbyteCatalog, ConfiguredAirbyteStream, DestinationSyncMode, Status, SyncMode, Type


@pytest.mark.parametrize("sync_mode", [SyncMode.incremental, SyncMode.full_refresh])
def test_live_snapshot_and_resume(sync_mode):
    configured_path = os.environ.get("CONVEX_TEST_CONFIG")
    config_path = Path(configured_path or "secrets/config.json")
    if configured_path is None and not config_path.is_file():
        pytest.skip("Set CONVEX_TEST_CONFIG to a disposable Convex deployment's config.json")
    config = json.loads(config_path.read_text())
    config["max_pages_per_sync"] = 0
    source = SourceConvexDataSync()
    logger = logging.getLogger("airbyte")
    connection = source.check(logger, config)
    assert connection.status == Status.SUCCEEDED, connection.message
    streams = source.discover(logger, config).streams
    assert any(stream.namespace for stream in streams), "Select a seeded component table"
    assert any(not stream.namespace for stream in streams), "Select a seeded root table"
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=stream,
                sync_mode=sync_mode,
                destination_sync_mode=DestinationSyncMode.overwrite
                if sync_mode == SyncMode.full_refresh
                else DestinationSyncMode.append_dedup,
                cursor_field=["_ts"],
                primary_key=[["_id"]],
            )
            for stream in streams
        ]
    )
    schemas = {(stream.namespace, stream.name): Draft7Validator(stream.json_schema) for stream in streams}
    state = None
    for initial in (True, False):
        seen = set()
        checkpoints = {}
        for message in source.read(logger, config, catalog, state):
            if message.type == Type.RECORD:
                record = message.record
                key = (record.namespace, record.stream)
                schemas[key].validate(record.data)
                assert record.data["_component"] == (record.namespace or "")
                assert record.data["_table"] == record.stream
                assert record.data["_ab_cdc_lsn"] == record.data["_ts"]
                assert (record.data["_ab_cdc_deleted_at"] is not None) == record.data["_deleted"]
                seen.add(key)
            elif message.type == Type.STATE:
                descriptor = message.state.stream.stream_descriptor
                checkpoints[(descriptor.namespace, descriptor.name)] = message.state
        if initial or sync_mode == SyncMode.full_refresh:
            assert seen == set(schemas), "Every selected table must contain at least one fixture document"
        assert set(checkpoints) == set(schemas)
        blobs = [vars(checkpoint.stream.stream_state) for checkpoint in checkpoints.values()]
        assert all(blob["snapshot_complete"] and not blob["awaiting_truncate"] for blob in blobs)
        assert len({blob["cursor"] for blob in blobs}) == 1
        state = list(checkpoints.values())
