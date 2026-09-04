#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging

import pytest
from source_convex_data_sync.source import SourceConvexDataSync

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
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType
from unit_tests.helpers import SYNC_URL, page, value


logger = logging.getLogger("airbyte")


def run(config, catalog, state=None):
    messages = list(SourceConvexDataSync().read(logger, config, catalog, state))
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
        "_other": "excluded",
        "": {"_other": "excluded", "posts": {"_other": "included"}},
        "betterAuth": {"_other": "excluded", "user": {"_other": "included"}},
        "resend/rateLimiter": {"_other": "excluded", "rateLimits": {"_other": "included"}},
    }
    assert [r.json().get("cursor") for r in requests_mock.request_history] == [None, "c1", "c2"]

    # Stopped on the first upToDate page even though it carried values and hasMore stayed true.
    assert len(requests_mock.request_history) == 3

    # Final checkpoint: every stream carries the same last cursor.
    assert all(s.type == AirbyteStateType.STREAM for s in states)
    stream_states = last_states(states)
    assert set(stream_states) == {"posts", "betterAuth__user", "resend__rateLimiter__rateLimits"}
    assert {s["cursor"] for s in stream_states.values()} == {"c3"}
    assert {s["sync_id"] for s in stream_states.values()} == {"sync-1"}
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
    assert second["selection"]["betterAuth"] == {"_other": "excluded", "user": {"_other": "included"}}


def test_read_full_refresh_is_reselected_even_when_priming_page_is_up_to_date(requests_mock, inline_config, catalog, caplog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh  # betterAuth__user
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="c2", status={"type": "upToDate", "snapshotTs": 1})},
            {
                "json": page(
                    truncates=[{"component": "betterAuth", "table": "user"}],
                    values=[value("betterAuth", "user", {"_id": "u1", "_creationTime": 1.0, "email": "a@b.c"})],
                    cursor="c3",
                    status={"type": "upToDate", "snapshotTs": 2},
                )
            },
        ],
    )
    with caplog.at_level(logging.WARNING):
        _, records, states, _ = run(inline_config, catalog, global_state("c1"))
    assert len(requests_mock.request_history) == 2
    assert "betterAuth" not in requests_mock.request_history[0].json()["selection"]
    assert "betterAuth" in requests_mock.request_history[1].json()["selection"]
    assert [r.stream for r in records] == ["betterAuth__user"]
    assert last_states(states)["betterAuth__user"]["snapshot_complete"] is True
    # The truncate that starts the re-sync is expected, not a post-snapshot table replacement.
    assert "truncated table" not in caplog.text


def test_read_all_full_refresh_still_fetches_full_selection(requests_mock, inline_config, catalog):
    for cs in catalog.streams:
        cs.sync_mode = SyncMode.full_refresh
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c2", status={"type": "upToDate", "snapshotTs": 1})}])
    run(inline_config, catalog, global_state("c1"))
    assert requests_mock.request_history[0].json()["selection"] == {"_other": "excluded"}
    assert "posts" in requests_mock.request_history[1].json()["selection"][""]


def test_read_max_pages_does_not_stop_on_priming_page(requests_mock, inline_config, catalog):
    inline_config["max_pages_per_sync"] = 1
    catalog.streams[0].sync_mode = SyncMode.full_refresh  # posts
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c2")}, {"json": page(cursor="c3")}])
    run(inline_config, catalog, global_state("c1"))
    assert len(requests_mock.request_history) == 2


def test_read_resnapshots_streams_whose_state_was_cleared(requests_mock, inline_config, catalog):
    # The platform's per-stream "Clear data" passes state for the other streams only.
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c9", status={"type": "upToDate", "snapshotTs": 1})}])
    run(inline_config, catalog, [stream_state("posts", "c8"), stream_state("betterAuth__user", "c8")])
    first, second = requests_mock.request_history[0].json(), requests_mock.request_history[1].json()
    assert first["cursor"] == "c8"
    assert "resend/rateLimiter" not in first["selection"]
    assert "resend/rateLimiter" in second["selection"]


def test_read_skips_streams_missing_from_the_source(requests_mock, inline_config, catalog):
    from unit_tests.helpers import POSTS_SCHEMA, configured_stream

    catalog.streams.append(configured_stream("comments", "", "comments", POSTS_SCHEMA))
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c1", status={"type": "upToDate", "snapshotTs": 1})}])
    messages, _, states, _ = run(inline_config, catalog)
    assert "comments" not in requests_mock.request_history[0].json()["selection"][""]
    incomplete = [m.trace.stream_status for m in messages if m.type == Type.TRACE and m.trace.stream_status]
    assert [(s.stream_descriptor.name, s.status) for s in incomplete if s.status == AirbyteStreamStatus.INCOMPLETE] == [
        ("comments", AirbyteStreamStatus.INCOMPLETE)
    ]
    assert "comments" not in last_states(states)


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


def test_read_invalid_cursor_is_a_config_error(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, [{"status_code": 400, "json": {"code": "InvalidDataSyncCursor", "message": "Could not parse"}}])
    with pytest.raises(AirbyteTracedException) as err:
        list(SourceConvexDataSync().read(logger, inline_config, catalog, global_state("from-other-deployment")))
    assert err.value.failure_type == FailureType.config_error
    assert "deployment URL" in err.value.message
    assert len(requests_mock.request_history) == 1


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
        for message in SourceConvexDataSync().read(logger, inline_config, catalog, None):
            emitted.append(message)
    assert "deployment:data:view" in str(err.value.message)
    assert err.value.failure_type == FailureType.config_error
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
    monkeypatch.setattr("source_convex_data_sync.source.time.sleep", lambda _: None)
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
    state = [
        stream_state("posts", "old", checkpointed_at=1),
        stream_state("betterAuth__user", "newer", checkpointed_at=2),
        stream_state("resend__rateLimiter__rateLimits", "old", checkpointed_at=1),
    ]
    run(inline_config, catalog, state)
    assert requests_mock.request_history[0].json()["cursor"] == "newer"


def test_read_with_empty_catalog_makes_no_requests(requests_mock, inline_config, catalog):
    catalog.streams = []
    messages, *_ = run(inline_config, catalog)
    assert messages == []
    assert not requests_mock.called


def test_read_drops_tombstones_from_full_refresh_streams(requests_mock, inline_config, catalog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh  # betterAuth__user
    requests_mock.post(
        SYNC_URL,
        [
            {
                "json": page(
                    values=[
                        value("betterAuth", "user", {"_id": "u1", "_creationTime": 1.0, "email": "a@b.c"}, ts=10),
                        value("betterAuth", "user", {"_id": "u2"}, ts=20, deleted=True),
                        value("", "posts", {"_id": "p1"}, ts=30, deleted=True),
                    ],
                    cursor="c1",
                    status={"type": "upToDate", "snapshotTs": 1},
                )
            }
        ],
    )
    _, records, _, statuses = run(inline_config, catalog)
    assert [(r.stream, r.data["_id"], r.data["_deleted"]) for r in records] == [("betterAuth__user", "u1", False), ("posts", "p1", True)]
    assert statuses.count(AirbyteStreamStatus.COMPLETE) == 3


def test_read_continues_an_in_progress_full_refresh_snapshot(requests_mock, inline_config, catalog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh  # betterAuth__user
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c2", status={"type": "upToDate", "snapshotTs": 1})}])
    state = [
        stream_state(name, "c1", snapshot_complete=(name != "betterAuth__user"))
        for name in ("posts", "betterAuth__user", "resend__rateLimiter__rateLimits")
    ]
    _, _, states, statuses = run(inline_config, catalog, state)
    # No priming page: the previous run's re-sync of betterAuth__user is still tracked by Convex and just carries on.
    assert len(requests_mock.request_history) == 1
    assert "betterAuth" in requests_mock.request_history[0].json()["selection"]
    assert last_states(states)["betterAuth__user"]["snapshot_complete"] is True
    assert AirbyteStreamStatus.INCOMPLETE not in statuses


def test_read_reports_partial_full_refresh_snapshot_incomplete_at_max_pages(requests_mock, inline_config, catalog, caplog):
    inline_config["max_pages_per_sync"] = 1
    catalog.streams[0].sync_mode = SyncMode.full_refresh  # posts
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c1")}, {"json": page(cursor="c2")}])
    with caplog.at_level(logging.WARNING):
        messages, _, states, _ = run(inline_config, catalog)
    final = {
        m.trace.stream_status.stream_descriptor.name: m.trace.stream_status.status
        for m in messages
        if m.type == Type.TRACE and m.trace.stream_status
    }
    assert final["posts"] == AirbyteStreamStatus.INCOMPLETE
    assert final["betterAuth__user"] == AirbyteStreamStatus.COMPLETE
    assert last_states(states)["posts"]["snapshot_complete"] is False
    assert "full refresh snapshot of ['posts']" in caplog.text


def test_read_resnapshots_streams_with_stale_state_without_warning(requests_mock, inline_config, catalog, caplog):
    # betterAuth__user was disabled for a few runs (the platform kept its old state) and is enabled again.
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="c9")},
            {
                "json": page(
                    truncates=[{"component": "betterAuth", "table": "user"}], cursor="c10", status={"type": "upToDate", "snapshotTs": 1}
                )
            },
        ],
    )
    state = [
        stream_state("posts", "c8", checkpointed_at=8, snapshot_complete=True),
        stream_state("betterAuth__user", "c3", checkpointed_at=3, snapshot_complete=True),
        stream_state("resend__rateLimiter__rateLimits", "c8", checkpointed_at=8, snapshot_complete=True),
    ]
    with caplog.at_level(logging.WARNING):
        run(inline_config, catalog, state)
    first, second = requests_mock.request_history[0].json(), requests_mock.request_history[1].json()
    assert first["cursor"] == "c8" and "betterAuth" not in first["selection"]
    assert "betterAuth" in second["selection"]
    assert "truncated table" not in caplog.text


def test_read_treats_stream_whose_hint_disagrees_with_its_name_as_missing(requests_mock, inline_config, catalog):
    from unit_tests.helpers import USER_SCHEMA, configured_stream

    # The name still resolves to root/posts, but the catalog entry was discovered as betterAuth/user.
    catalog.streams[0] = configured_stream("posts", "betterAuth", "user", USER_SCHEMA)
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c1", status={"type": "upToDate", "snapshotTs": 1})}])
    messages, _, states, _ = run(inline_config, catalog)
    assert "" not in requests_mock.request_history[0].json()["selection"]
    incomplete = [
        m.trace.stream_status.stream_descriptor.name
        for m in messages
        if m.type == Type.TRACE and m.trace.stream_status and m.trace.stream_status.status == AirbyteStreamStatus.INCOMPLETE
    ]
    assert incomplete == ["posts"]
    assert "posts" not in last_states(states)


def test_read_does_not_retry_a_malformed_deployment_url(inline_config, catalog, monkeypatch):
    sleeps = []
    monkeypatch.setattr("source_convex_data_sync.source.time.sleep", sleeps.append)
    inline_config["deployment_url"] = "murky-swan-635.convex.cloud"
    with pytest.raises(AirbyteTracedException) as err:
        run(inline_config, catalog)
    assert err.value.failure_type == FailureType.config_error
    assert "No scheme supplied" in err.value.message
    assert sleeps == []
