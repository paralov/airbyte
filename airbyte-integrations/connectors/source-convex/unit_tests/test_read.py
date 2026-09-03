#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging

import pytest
from source_convex.source import SourceConvex

from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    AirbyteStreamStatus,
    StreamDescriptor,
    SyncMode,
    Type,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from unit_tests.helpers import SYNC_URL, page, value


logger = logging.getLogger("airbyte")


def run(config, catalog, state=None):
    messages = list(SourceConvex().read(logger, config, catalog, state))
    records = [m.record for m in messages if m.type == Type.RECORD]
    states = [m.state for m in messages if m.type == Type.STATE]
    statuses = [m.trace.stream_status.status for m in messages if m.type == Type.TRACE and m.trace.stream_status is not None]
    return messages, records, states, statuses


def stream_state(name, cursor, sync_id="sync-1", checkpointed_at=1, **extra):
    return AirbyteStateMessage(
        type=AirbyteStateType.STREAM,
        stream=AirbyteStreamState(
            stream_descriptor=StreamDescriptor(name=name),
            stream_state=AirbyteStateBlob(cursor=cursor, sync_id=sync_id, checkpointed_at=checkpointed_at, **extra),
        ),
    )


def global_state(cursor, sync_id="sync-1"):
    return [stream_state(name, cursor, sync_id) for name in ("posts", "betterAuth__user", "resend__rateLimiter__rateLimits")]


def shared(state):
    return dict(vars(state.stream.stream_state))


def last_states(states):
    """The final checkpoint: one state message per stream, keyed by stream name."""
    out = {}
    for s in states:
        out[s.stream.stream_descriptor.name] = dict(vars(s.stream.stream_state))
    return out


def test_read_routes_records_by_component(requests_mock, inline_config, catalog):
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(truncates=[{"component": "", "table": "posts"}, {"component": "betterAuth", "table": "user"}], cursor="c1")},
            {
                "json": page(
                    values=[
                        value("", "posts", {"_id": "p1", "_creationTime": 1.0, "author": "u1", "body": "hi"}, ts=10_000_000_000),
                        value("betterAuth", "user", {"_id": "u1", "_creationTime": 2.0, "email": "a@b.c"}, ts=20_000_000_000),
                        value(
                            "resend/rateLimiter",
                            "rateLimits",
                            {"_id": "r1", "_creationTime": 3.0, "name": "x", "value": 1.0},
                            ts=30_000_000_000,
                        ),
                        value("someOther", "table", {"_id": "z1"}, ts=40_000_000_000),
                    ],
                    cursor="c2",
                )
            },
            {
                "json": page(
                    values=[value("betterAuth", "user", {"_id": "u1"}, ts=50_000_000_000, deleted=True)],
                    cursor="c3",
                    status={"type": "upToDate", "snapshotTs": 123},
                )
            },
            {"json": page(cursor="c4", status={"type": "upToDate", "snapshotTs": 123})},
        ],
    )

    messages, records, states, statuses = run(inline_config, catalog)

    assert [r.stream for r in records] == ["posts", "betterAuth__user", "resend__rateLimiter__rateLimits", "betterAuth__user"]
    post = records[0].data
    assert post["_id"] == "p1" and post["body"] == "hi"
    assert post["_ts"] == 10_000_000_000 and post["_ab_cdc_lsn"] == 10_000_000_000
    assert post["_component"] == "" and post["_table"] == "posts"
    assert post["_deleted"] is False and post["_ab_cdc_deleted_at"] is None
    assert post["_ab_cdc_updated_at"] == "1970-01-01T00:00:10"

    tombstone = records[3].data
    assert tombstone == {
        "_id": "u1",
        "_ts": 50_000_000_000,
        "_deleted": True,
        "_component": "betterAuth",
        "_table": "user",
        "_ab_cdc_lsn": 50_000_000_000,
        "_ab_cdc_updated_at": "1970-01-01T00:00:50",
        "_ab_cdc_deleted_at": "1970-01-01T00:00:50",
    }

    # Selection was sent on every request and only covers configured streams.
    first = requests_mock.request_history[0].json()
    assert "cursor" not in first
    assert first["selection"] == {
        "_other": "excl",
        "": {"_other": "excl", "posts": {"_other": "incl"}},
        "betterAuth": {"_other": "excl", "user": {"_other": "incl"}},
        "resend/rateLimiter": {"_other": "excl", "rateLimits": {"_other": "incl"}},
    }
    assert [r.json().get("cursor") for r in requests_mock.request_history] == [None, "c1", "c2", "c3"]

    # Stopped on the first empty upToDate page even though hasMore stayed true.
    assert len(requests_mock.request_history) == 4

    # Final checkpoint: every stream carries the same last cursor.
    assert all(s.type == AirbyteStateType.STREAM for s in states)
    stream_states = last_states(states)
    assert set(stream_states) == {"posts", "betterAuth__user", "resend__rateLimiter__rateLimits"}
    assert {s["cursor"] for s in stream_states.values()} == {"c4"}
    assert {s["sync_id"] for s in stream_states.values()} == {"sync-1"}
    assert stream_states["betterAuth__user"]["last_ts"] == 50_000_000_000
    assert stream_states["betterAuth__user"]["snapshot_complete"] is True

    # Stream status lifecycle.
    assert statuses.count(AirbyteStreamStatus.STARTED) == 3
    assert statuses.count(AirbyteStreamStatus.COMPLETE) == 3
    assert AirbyteStreamStatus.INCOMPLETE not in statuses


def test_read_resumes_from_global_state(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c9", status={"type": "upToDate", "snapshotTs": 1})}])
    _, records, states, _ = run(inline_config, catalog, global_state("c8"))
    assert requests_mock.request_history[0].json()["cursor"] == "c8"
    assert records == []
    assert shared(states[-1])["cursor"] == "c9"


def test_read_full_refresh_stream_is_reselected(requests_mock, inline_config, catalog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh  # betterAuth__user
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="c2")},
            {"json": page(truncates=[{"component": "betterAuth", "table": "user"}], cursor="c3")},
            {"json": page(cursor="c4", status={"type": "upToDate", "snapshotTs": 1})},
        ],
    )
    run(inline_config, catalog, global_state("c1"))
    first, second = requests_mock.request_history[0].json(), requests_mock.request_history[1].json()
    assert "betterAuth" not in first["selection"]
    assert second["selection"]["betterAuth"] == {"_other": "excl", "user": {"_other": "incl"}}


def test_read_restarts_on_expired_cursor(requests_mock, inline_config, catalog):
    requests_mock.post(
        SYNC_URL,
        [
            {"status_code": 400, "json": {"code": "DataSyncCursorExpired", "message": "expired"}},
            {
                "json": page(
                    values=[value("", "posts", {"_id": "p1", "_creationTime": 1.0, "author": "u", "body": "b"})],
                    cursor="n1",
                    sync_id="sync-2",
                )
            },
            {"json": page(cursor="n2", status={"type": "upToDate", "snapshotTs": 1}, sync_id="sync-2")},
        ],
    )
    _, records, states, _ = run(inline_config, catalog, global_state("stale"))
    assert requests_mock.request_history[0].json()["cursor"] == "stale"
    assert "cursor" not in requests_mock.request_history[1].json()
    assert [r.data["_id"] for r in records] == ["p1"]
    assert shared(states[-1])["cursor"] == "n2"
    assert shared(states[-1])["sync_id"] == "sync-2"


def test_read_surfaces_other_errors_with_state(requests_mock, inline_config, catalog):
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="c1")},
            {"status_code": 403, "json": {"code": "Forbidden", "message": "missing deployment:data:view"}},
        ],
    )
    emitted = []
    with pytest.raises(AirbyteTracedException) as err:
        for message in SourceConvex().read(logger, inline_config, catalog, None):
            emitted.append(message)
    assert "deployment:data:view" in str(err.value.message)
    states = [m.state for m in emitted if m.type == Type.STATE]
    assert shared(states[-1])["cursor"] == "c1"
    statuses = [m.trace.stream_status.status for m in emitted if m.type == Type.TRACE and m.trace.stream_status]
    assert statuses.count(AirbyteStreamStatus.INCOMPLETE) == 3


def test_read_honours_max_pages(requests_mock, inline_config, catalog):
    inline_config["max_pages_per_sync"] = 2
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c1")}, {"json": page(cursor="c2")}, {"json": page(cursor="c3")}])
    _, _, states, statuses = run(inline_config, catalog)
    assert len(requests_mock.request_history) == 2
    assert shared(states[-1])["cursor"] == "c2"
    assert statuses.count(AirbyteStreamStatus.COMPLETE) == 3


def test_read_checkpoints_every_n_pages(requests_mock, inline_config, catalog):
    inline_config["state_checkpoint_pages"] = 2
    requests_mock.post(
        SYNC_URL,
        [{"json": page(cursor=f"c{i}")} for i in range(1, 6)]
        + [{"json": page(cursor="done", status={"type": "upToDate", "snapshotTs": 1})}],
    )
    _, _, states, _ = run(inline_config, catalog)
    # 3 streams per checkpoint: after page 2, page 4, the final page (6), and the end-of-sync checkpoint.
    assert [shared(s)["cursor"] for s in states] == ["c2"] * 3 + ["c4"] * 3 + ["done"] * 6


def test_read_retries_on_server_errors(requests_mock, inline_config, catalog, monkeypatch):
    monkeypatch.setattr("source_convex.source.time.sleep", lambda _: None)
    requests_mock.post(
        SYNC_URL,
        [
            {"status_code": 503, "text": "unavailable"},
            {"status_code": 429, "text": "slow down", "headers": {"Retry-After": "0"}},
            {"json": page(cursor="c1", status={"type": "upToDate", "snapshotTs": 1})},
        ],
    )
    _, _, states, _ = run(inline_config, catalog)
    assert len(requests_mock.request_history) == 3
    assert shared(states[-1])["cursor"] == "c1"


def test_read_resumes_from_most_recent_checkpoint(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c9", status={"type": "upToDate", "snapshotTs": 1})}])
    state = [stream_state("posts", "old", checkpointed_at=1), stream_state("betterAuth__user", "newer", checkpointed_at=2)]
    run(inline_config, catalog, state)
    assert requests_mock.request_history[0].json()["cursor"] == "newer"


def test_read_ignores_legacy_per_stream_state(requests_mock, inline_config, catalog):
    legacy = [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="posts"),
                stream_state=AirbyteStateBlob(snapshot_cursor="x", snapshot_has_more=False, delta_cursor=1),
            ),
        )
    ]
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c1", status={"type": "upToDate", "snapshotTs": 1})}])
    run(inline_config, catalog, legacy)
    assert "cursor" not in requests_mock.request_history[0].json()


def test_read_with_empty_catalog_makes_no_requests(requests_mock, inline_config, catalog):
    catalog.streams = []
    messages, *_ = run(inline_config, catalog)
    assert messages == []
    assert not requests_mock.called
