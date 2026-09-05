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
from unit_tests.helpers import (
    POSTS,
    POSTS_SCHEMA,
    RATE_LIMITS,
    SYNC_URL,
    USER,
    USER_SCHEMA,
    configured_stream,
    ident,
    page,
    value,
)


logger = logging.getLogger("airbyte")

# Convex truncates every selected table on the page where its traversal begins, so a cold start sends these.
POSTS_TRUNCATE = {"component": "", "table": "posts"}
USER_TRUNCATE = {"component": "betterAuth", "table": "user"}
RATE_LIMITS_TRUNCATE = {"component": "resend/rateLimiter", "table": "rateLimits"}
ALL_TRUNCATES = [POSTS_TRUNCATE, USER_TRUNCATE, RATE_LIMITS_TRUNCATE]


def run(config, catalog, state=None):
    messages = list(SourceConvexDataSync().read(logger, config, catalog, state))
    records = [m.record for m in messages if m.type == Type.RECORD]
    states = [m.state for m in messages if m.type == Type.STATE]
    statuses = [m.trace.stream_status.status for m in messages if m.type == Type.TRACE and m.trace.stream_status is not None]
    return messages, records, states, statuses


def stream_state(key, cursor, sync_id="sync-1", checkpointed_at=1, **extra):
    namespace, name = key
    return AirbyteStateMessage(
        type=AirbyteStateType.STREAM,
        stream=AirbyteStreamState(
            stream_descriptor=StreamDescriptor(name=name, namespace=namespace),
            stream_state=AirbyteStateBlob(cursor=cursor, sync_id=sync_id, checkpointed_at=checkpointed_at, **extra),
        ),
    )


def saved_states(cursor, sync_id="sync-1"):
    return [stream_state(name, cursor, sync_id) for name in (POSTS, USER, RATE_LIMITS)]


def shared(state):
    return dict(vars(state.stream.stream_state))


def last_states(states):
    """The final checkpoint: one state message per stream, keyed by stream name."""
    out = {}
    for s in states:
        out[ident(s.stream.stream_descriptor)] = dict(vars(s.stream.stream_state))
    return out


def test_read_routes_records_by_component(requests_mock, inline_config, catalog):
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(truncates=ALL_TRUNCATES, cursor="c1")},
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
                    values=[value("betterAuth", "user", {"_id": "u1"}, ts=1_700_000_000_999_999_999, deleted=True)],
                    cursor="c3",
                    status={"type": "upToDate", "snapshotTs": 123},
                )
            },
            {"json": page(cursor="c4", status={"type": "upToDate", "snapshotTs": 123})},
        ],
    )

    messages, records, states, statuses = run(inline_config, catalog)

    assert [ident(r) for r in records] == [POSTS, USER, RATE_LIMITS, USER]
    post = records[0].data
    assert post["_id"] == "p1" and post["body"] == "hi"
    assert post["_ts"] == 10_000_000_000 and post["_ab_cdc_lsn"] == 10_000_000_000
    assert post["_component"] == "" and post["_table"] == "posts"
    assert post["_deleted"] is False and post["_ab_cdc_deleted_at"] is None
    assert post["_ab_cdc_updated_at"] == "1970-01-01T00:00:10"

    tombstone = records[3].data
    assert tombstone == {
        "_id": "u1",
        "_ts": 1_700_000_000_999_999_999,
        "_deleted": True,
        "_component": "betterAuth",
        "_table": "user",
        "_ab_cdc_lsn": 1_700_000_000_999_999_999,
        "_ab_cdc_updated_at": "2023-11-14T22:13:20.999999",
        "_ab_cdc_deleted_at": "2023-11-14T22:13:20.999999",
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
    assert set(stream_states) == {POSTS, USER, RATE_LIMITS}
    assert {s["cursor"] for s in stream_states.values()} == {"c3"}
    assert {s["sync_id"] for s in stream_states.values()} == {"sync-1"}
    assert stream_states[USER]["snapshot_complete"] is True

    # Stream status lifecycle.
    assert statuses.count(AirbyteStreamStatus.STARTED) == 3
    assert statuses.count(AirbyteStreamStatus.COMPLETE) == 3
    assert AirbyteStreamStatus.INCOMPLETE not in statuses


def test_read_resumes_from_saved_states(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c9", status={"type": "upToDate", "snapshotTs": 1})}])
    _, records, states, _ = run(inline_config, catalog, saved_states("c8"))
    assert requests_mock.request_history[0].json()["cursor"] == "c8"
    assert records == []
    assert shared(states[-1])["cursor"] == "c9"


def test_read_full_refresh_stream_is_reselected(requests_mock, inline_config, catalog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh  # betterAuth/user
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="c2")},
            {"json": page(truncates=[{"component": "betterAuth", "table": "user"}], cursor="c3")},
            {"json": page(cursor="c4", status={"type": "upToDate", "snapshotTs": 1})},
        ],
    )
    run(inline_config, catalog, saved_states("c1"))
    first, second = requests_mock.request_history[0].json(), requests_mock.request_history[1].json()
    assert "betterAuth" not in first["selection"]
    assert second["selection"]["betterAuth"] == {"_other": "excluded", "user": {"_other": "included"}}


def test_read_full_refresh_is_reselected_even_when_priming_page_is_up_to_date(requests_mock, inline_config, catalog, caplog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh  # betterAuth/user
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
        _, records, states, _ = run(inline_config, catalog, saved_states("c1"))
    assert len(requests_mock.request_history) == 2
    assert "betterAuth" not in requests_mock.request_history[0].json()["selection"]
    assert "betterAuth" in requests_mock.request_history[1].json()["selection"]
    assert [ident(r) for r in records] == [USER]
    assert last_states(states)[USER]["snapshot_complete"] is True
    # The truncate that starts the re-sync is expected, not a post-snapshot table replacement.
    assert "truncated table" not in caplog.text


def test_read_all_full_refresh_still_fetches_full_selection(requests_mock, inline_config, catalog):
    for cs in catalog.streams:
        cs.sync_mode = SyncMode.full_refresh
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="c2", status={"type": "upToDate"})},
            {"json": page(truncates=ALL_TRUNCATES, cursor="c3", status={"type": "upToDate"})},
        ],
    )
    _, records, states, statuses = run(inline_config, catalog, saved_states("c1"))
    assert requests_mock.request_history[0].json()["selection"] == {"_other": "excluded"}
    assert "posts" in requests_mock.request_history[1].json()["selection"][""]
    assert records == []
    assert set(last_states(states)) == {POSTS, USER, RATE_LIMITS}
    assert statuses.count(AirbyteStreamStatus.COMPLETE) == 3
    assert AirbyteStreamStatus.INCOMPLETE not in statuses


def test_read_max_pages_does_not_stop_on_priming_page(requests_mock, inline_config, catalog):
    inline_config["max_pages_per_sync"] = 1
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c2")}, {"json": page(cursor="c3")}])
    # resend/rateLimiter/rateLimits has no state, so it is re-synced via a priming page first.
    run(inline_config, catalog, [stream_state(POSTS, "c1"), stream_state(USER, "c1")])
    assert len(requests_mock.request_history) == 2


def test_read_ignores_max_pages_while_a_full_refresh_snapshot_is_in_progress(requests_mock, inline_config, catalog, caplog):
    inline_config["max_pages_per_sync"] = 1
    catalog.streams[0].sync_mode = SyncMode.full_refresh  # posts
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="c2")},  # priming page
            {"json": page(truncates=[{"component": "", "table": "posts"}], cursor="c3")},
            {"json": page(cursor="c4")},
            {"json": page(cursor="c5", status={"type": "upToDate", "snapshotTs": 1})},
        ],
    )
    with caplog.at_level(logging.INFO):
        _, _, states, statuses = run(inline_config, catalog, saved_states("c1"))
    # The platform clears full refresh state between jobs, so a snapshot cut short by the page cap would start over
    # on every run; the run continues until the sync is up to date instead.
    assert len(requests_mock.request_history) == 4
    assert last_states(states)[POSTS]["snapshot_complete"] is True
    assert statuses.count(AirbyteStreamStatus.COMPLETE) == 3 and AirbyteStreamStatus.INCOMPLETE not in statuses
    assert "max_pages_per_sync=1 is not applied" in caplog.text


def test_read_does_not_checkpoint_when_the_priming_page_fails(requests_mock, inline_config, catalog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh  # betterAuth/user
    requests_mock.post(SYNC_URL, [{"status_code": 403, "json": {"code": "Forbidden", "message": "missing deployment:data:view"}}])
    emitted = []
    with pytest.raises(AirbyteTracedException):
        for message in SourceConvexDataSync().read(logger, inline_config, catalog, saved_states("c1")):
            emitted.append(message)
    # The cursor never moved: persisting {cursor: c1, snapshot_complete: false} would make the next attempt skip the
    # priming page and report the full refresh stream complete after receiving only deltas.
    assert [m for m in emitted if m.type == Type.STATE] == []
    statuses = [m.trace.stream_status.status for m in emitted if m.type == Type.TRACE and m.trace.stream_status]
    assert statuses.count(AirbyteStreamStatus.INCOMPLETE) == 3


def test_read_refuses_a_cursor_saved_for_another_deployment(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, [{"json": page(truncates=ALL_TRUNCATES, cursor="c9", status={"type": "upToDate", "snapshotTs": 1})}])
    _, _, states, _ = run(inline_config, catalog)
    assert {shared(s)["deployment_url"] for s in states} == {"https://murky-swan-635.convex.cloud"}

    inline_config["deployment_url"] = "https://cluttered-owl-337.convex.cloud/"
    with pytest.raises(AirbyteTracedException) as err:
        run(inline_config, catalog, states)
    assert err.value.failure_type == FailureType.config_error
    assert "murky-swan-635" in err.value.message and "cluttered-owl-337" in err.value.message
    assert len(requests_mock.request_history) == 1  # no request went to the new deployment


def test_read_rejects_a_page_without_a_next_cursor(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c1")}, {"json": page(cursor=None)}])
    emitted = []
    with pytest.raises(AirbyteTracedException) as err:
        for message in SourceConvexDataSync().read(logger, inline_config, catalog, None):
            emitted.append(message)
    assert "nextCursor" in err.value.message
    # The previous good cursor is checkpointed, so the next run resumes instead of silently starting a new sync.
    states = [m.state for m in emitted if m.type == Type.STATE]
    assert shared(states[-1])["cursor"] == "c1"


def test_read_resnapshots_streams_whose_state_was_cleared(requests_mock, inline_config, catalog):
    # The platform's per-stream "Clear data" passes state for the other streams only.
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c9", status={"type": "upToDate", "snapshotTs": 1})}])
    run(inline_config, catalog, [stream_state(POSTS, "c8"), stream_state(USER, "c8")])
    first, second = requests_mock.request_history[0].json(), requests_mock.request_history[1].json()
    assert first["cursor"] == "c8"
    assert "resend/rateLimiter" not in first["selection"]
    assert "resend/rateLimiter" in second["selection"]


def test_read_skips_streams_missing_from_the_source(requests_mock, inline_config, catalog):
    catalog.streams.append(configured_stream("", "comments", POSTS_SCHEMA))
    requests_mock.post(SYNC_URL, [{"json": page(truncates=ALL_TRUNCATES, cursor="c1", status={"type": "upToDate", "snapshotTs": 1})}])
    messages, _, states, _ = run(inline_config, catalog)
    assert "comments" not in requests_mock.request_history[0].json()["selection"][""]
    incomplete = [m.trace.stream_status for m in messages if m.type == Type.TRACE and m.trace.stream_status]
    assert [(s.stream_descriptor.name, s.status) for s in incomplete if s.status == AirbyteStreamStatus.INCOMPLETE] == [
        ("comments", AirbyteStreamStatus.INCOMPLETE)
    ]
    assert (None, "comments") not in last_states(states)


def test_read_expired_cursor_requires_reset_instead_of_losing_deletes(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, status_code=400, json={"code": "DataSyncCursorExpired", "message": "expired"})
    emitted = []
    with pytest.raises(AirbyteTracedException) as err:
        emitted.extend(SourceConvexDataSync().read(logger, inline_config, catalog, saved_states("expired")))
    assert err.value.failure_type == FailureType.config_error
    assert "expired" in err.value.message and "Clear the connection's data" in err.value.message
    assert len(requests_mock.request_history) == 1
    assert not any(m.type in (Type.STATE, Type.RECORD) for m in emitted)


def test_read_invalid_cursor_is_a_config_error(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, [{"status_code": 400, "json": {"code": "InvalidDataSyncCursor", "message": "Could not parse"}}])
    with pytest.raises(AirbyteTracedException) as err:
        list(SourceConvexDataSync().read(logger, inline_config, catalog, saved_states("from-other-deployment")))
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
        [{"json": page(truncates=ALL_TRUNCATES if i == 1 else (), cursor=f"c{i}")} for i in range(1, 6)]
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
            {"json": page(truncates=ALL_TRUNCATES, cursor="c1", status={"type": "upToDate", "snapshotTs": 1})},
        ],
    )
    _, _, states, _ = run(inline_config, catalog)
    assert len(requests_mock.request_history) == 3
    assert shared(states[-1])["cursor"] == "c1"


@pytest.mark.parametrize("lagging", [POSTS, USER, RATE_LIMITS])
def test_read_replays_unacknowledged_deletes(requests_mock, inline_config, catalog, lagging):
    def replay(request, context):
        assert request.json()["cursor"] == "old"
        assert all(table in request.json()["selection"][component or ""] for component, table in (POSTS, USER, RATE_LIMITS))
        return page(
            values=[value(lagging[0] or "", lagging[1], {"_id": "deleted-row"}, deleted=True)],
            cursor="next",
            status={"type": "upToDate"},
        )

    requests_mock.post(SYNC_URL, json=replay)
    state = [
        stream_state(key, "old" if key == lagging else "new", checkpointed_at=1 if key == lagging else 2, snapshot_complete=True)
        for key in (POSTS, USER, RATE_LIMITS)
    ]
    _, records, states, _ = run(inline_config, catalog, state)
    assert [(ident(r), r.data["_id"], r.data["_deleted"]) for r in records] == [(lagging, "deleted-row", True)]
    assert {shared(s)["checkpointed_at"] for s in states} == {3}


def test_read_with_empty_catalog_makes_no_requests(requests_mock, inline_config, catalog):
    catalog.streams = []
    messages, *_ = run(inline_config, catalog)
    assert messages == []
    assert not requests_mock.called


def test_read_drops_tombstones_from_full_refresh_streams(requests_mock, inline_config, catalog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh  # betterAuth/user
    requests_mock.post(
        SYNC_URL,
        [
            {
                "json": page(
                    truncates=ALL_TRUNCATES,
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
    assert [(ident(r), r.data["_id"], r.data["_deleted"]) for r in records] == [(POSTS, "p1", True), (USER, "u1", False)]
    assert statuses.count(AirbyteStreamStatus.COMPLETE) == 3


def test_read_rebuilds_an_in_progress_full_refresh_snapshot(requests_mock, inline_config, catalog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="primed")},
            {
                "json": page(
                    truncates=[USER_TRUNCATE],
                    values=[value("betterAuth", "user", {"_id": "unchanged"})],
                    cursor="done",
                    status={"type": "upToDate"},
                )
            },
        ],
    )
    state = [stream_state(name, "c1", snapshot_complete=(name != USER)) for name in (POSTS, USER, RATE_LIMITS)]
    _, records, states, statuses = run(inline_config, catalog, state)
    assert len(requests_mock.request_history) == 2
    assert "betterAuth" not in requests_mock.request_history[0].json()["selection"]
    assert records[0].data["_id"] == "unchanged"
    assert last_states(states)[USER]["snapshot_complete"] is True
    assert AirbyteStreamStatus.INCOMPLETE not in statuses


def test_read_checkpoint_stamps_continue_the_incoming_sequence(requests_mock, inline_config, catalog):
    # The stamp is a logical sequence, not wall-clock time, so a worker whose clock runs ahead can never make a stale
    # stream's checkpoint outrank the newest one.
    requests_mock.post(SYNC_URL, [{"json": page(cursor="c9", status={"type": "upToDate", "snapshotTs": 1})}])
    state = [
        stream_state(POSTS, "c8", checkpointed_at=1_788_466_011_116),
        stream_state(USER, "c8", checkpointed_at=1_788_466_011_116),
        stream_state(RATE_LIMITS, "c8", checkpointed_at=1_788_466_011_116),
    ]
    _, _, states, _ = run(inline_config, catalog, state)
    assert {shared(s)["checkpointed_at"] for s in states} == {1_788_466_011_117}


def test_read_treats_stream_whose_namespace_disagrees_with_the_schema_as_missing(requests_mock, inline_config, catalog):
    # The schema JSON knows root/posts, but the catalog entry claims the table lives in betterAuth.
    catalog.streams[0] = configured_stream("betterAuth", "posts", USER_SCHEMA)
    up_to_date = {"type": "upToDate", "snapshotTs": 1}
    requests_mock.post(SYNC_URL, [{"json": page(truncates=[USER_TRUNCATE, RATE_LIMITS_TRUNCATE], cursor="c1", status=up_to_date)}])
    messages, _, states, _ = run(inline_config, catalog)
    assert "" not in requests_mock.request_history[0].json()["selection"]
    incomplete = [
        m.trace.stream_status.stream_descriptor.name
        for m in messages
        if m.type == Type.TRACE and m.trace.stream_status and m.trace.stream_status.status == AirbyteStreamStatus.INCOMPLETE
    ]
    assert incomplete == ["posts"]
    assert ("betterAuth", "posts") not in last_states(states)


def test_read_reports_a_table_missing_from_the_deployment_incomplete(requests_mock, inline_config, catalog, caplog):
    # Convex only parses the selection; a table that does not exist never gets a truncate or a value.
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(truncates=[POSTS_TRUNCATE, USER_TRUNCATE], cursor="c1")},
            {"json": page(cursor="c2", status={"type": "upToDate", "snapshotTs": 1})},
        ],
    )
    with caplog.at_level(logging.WARNING):
        messages, _, states, statuses = run(inline_config, catalog)
    assert "resend/rateLimiter/rateLimits" in caplog.text and "do not exist" in caplog.text
    incomplete = [
        ident(m.trace.stream_status.stream_descriptor)
        for m in messages
        if m.type == Type.TRACE and m.trace.stream_status and m.trace.stream_status.status == AirbyteStreamStatus.INCOMPLETE
    ]
    assert incomplete == [RATE_LIMITS]
    assert statuses.count(AirbyteStreamStatus.COMPLETE) == 2
    assert last_states(states)[RATE_LIMITS]["awaiting_truncate"] is True
    assert last_states(states)[RATE_LIMITS]["snapshot_complete"] is False
    assert last_states(states)[POSTS]["snapshot_complete"] is True


def test_read_accepts_cosmetic_changes_to_the_deployment_url(requests_mock, inline_config, catalog):
    requests_mock.post(SYNC_URL, [{"json": page(truncates=ALL_TRUNCATES, cursor="c9", status={"type": "upToDate", "snapshotTs": 1})}])
    _, _, states, _ = run(inline_config, catalog)
    # Same host, different case, scheme and trailing slash: Convex would route this to the same deployment.
    inline_config["deployment_url"] = "HTTP://Murky-Swan-635.convex.cloud/"
    http_sync_url = "http://murky-swan-635.convex.cloud/api/v1/data/sync"
    requests_mock.post(http_sync_url, json=page(cursor="c10", status={"type": "upToDate", "snapshotTs": 2}))
    _, _, states, _ = run(inline_config, catalog, states)
    assert shared(states[-1])["cursor"] == "c10"


def test_read_does_not_retry_a_malformed_deployment_url(inline_config, catalog, monkeypatch):
    sleeps = []
    monkeypatch.setattr("source_convex_data_sync.source.time.sleep", sleeps.append)
    inline_config["deployment_url"] = "murky-swan-635.convex.cloud"
    with pytest.raises(AirbyteTracedException) as err:
        run(inline_config, catalog)
    assert err.value.failure_type == FailureType.config_error
    assert "InvalidDeploymentUrl" in err.value.message
    assert sleeps == []


@pytest.mark.parametrize("sync_mode", [SyncMode.incremental, SyncMode.full_refresh])
def test_read_through_the_cdk_entrypoint(requests_mock, inline_config, catalog, sync_mode):
    # Drives one sync through AirbyteEntrypoint so the state blobs and records are actually serialised and counted.
    from airbyte_cdk.test.entrypoint_wrapper import read as entrypoint_read

    requests_mock.post(
        SYNC_URL,
        [
            {
                "json": page(
                    truncates=ALL_TRUNCATES,
                    values=[value("", "posts", {"_id": "p1", "_creationTime": 1.0, "author": "u1", "body": "hi"})],
                    cursor="c1",
                )
            },
            {"json": page(cursor="c2", status={"type": "upToDate", "snapshotTs": 1})},
        ],
    )
    catalog.streams[0].sync_mode = sync_mode
    out = entrypoint_read(SourceConvexDataSync(), inline_config, catalog, None)
    assert out.errors == []
    assert [r.record.stream for r in out.records] == ["posts"]
    assert dict(vars(out.most_recent_state.stream_state))["cursor"] == "c2"
    checkpointed = {ident(s.state.stream.stream_descriptor) for s in out.state_messages}
    assert checkpointed == {POSTS, USER, RATE_LIMITS}
    assert sum(s.state.sourceStats.recordCount for s in out.state_messages) == 1


@pytest.mark.parametrize("stop", ["page_cap", "failure"])
@pytest.mark.parametrize("table_appears", [False, True])
def test_read_retains_pending_table_checks_on_resume(requests_mock, inline_config, catalog, caplog, stop, table_appears):
    inline_config["state_checkpoint_pages"] = 1
    if stop == "page_cap":
        inline_config["max_pages_per_sync"] = 1
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(truncates=[POSTS_TRUNCATE, USER_TRUNCATE], cursor="partial")},
            {"status_code": 403, "json": {"code": "Forbidden"}},
        ],
    )
    emitted = []
    try:
        emitted.extend(SourceConvexDataSync().read(logger, inline_config, catalog))
    except AirbyteTracedException:
        assert stop == "failure"
    states = [m.state for m in emitted if m.type == Type.STATE]
    requests_mock.post(
        SYNC_URL, json=page(truncates=[RATE_LIMITS_TRUNCATE] if table_appears else [], cursor="done", status={"type": "upToDate"})
    )
    with caplog.at_level(logging.WARNING):
        _, _, resumed_states, statuses = run(inline_config, catalog, states)
    assert requests_mock.last_request.json()["cursor"] == "partial"
    assert last_states(resumed_states)[RATE_LIMITS]["snapshot_complete"] is table_appears
    assert (AirbyteStreamStatus.INCOMPLETE in statuses) is not table_appears
    assert ("do not exist" in caplog.text) is not table_appears
    assert last_states(resumed_states)[POSTS]["snapshot_complete"] is True


def test_read_deselected_stream_does_not_hold_back_checkpoint(requests_mock, inline_config, catalog):
    catalog.streams = catalog.streams[:2]
    requests_mock.post(SYNC_URL, json=page(cursor="next", status={"type": "upToDate"}))
    state = [
        stream_state(POSTS, "current", checkpointed_at=5),
        stream_state(USER, "current", checkpointed_at=5),
        stream_state(RATE_LIMITS, "old", checkpointed_at=1),
    ]
    run(inline_config, catalog, state)
    assert requests_mock.last_request.json()["cursor"] == "current"


def test_read_keeps_rewind_point_across_repeated_partial_acknowledgements(requests_mock, inline_config, catalog):
    inline_config["max_pages_per_sync"] = 1
    state = [
        stream_state(POSTS, "c1", checkpointed_at=1, snapshot_complete=True),
        stream_state(USER, "c9", checkpointed_at=2, snapshot_complete=True),
        stream_state(RATE_LIMITS, "c9", checkpointed_at=2, snapshot_complete=True),
    ]
    # Recovery gets only as far as c5, behind the previous attempt's c9. Only posts
    # acknowledges this checkpoint. A sequence-number-only minimum would pick c9.
    requests_mock.post(SYNC_URL, json=page(cursor="c5"))
    _, _, states, _ = run(inline_config, catalog, state)
    state[0] = next(s for s in states if ident(s.stream.stream_descriptor) == POSTS)
    requests_mock.post(
        SYNC_URL,
        json=page(
            values=[value("", "posts", {"_id": "deleted-between-c5-and-c9"}, deleted=True)],
            cursor="c10",
            status={"type": "upToDate"},
        ),
    )
    _, records, states, _ = run(inline_config, catalog, state)
    assert requests_mock.last_request.json()["cursor"] == "c1"
    assert records[0].data["_id"] == "deleted-between-c5-and-c9"
    assert records[0].data["_deleted"] is True
    # Once every stream acknowledges c10, normal incremental progress resumes.
    requests_mock.post(SYNC_URL, json=page(cursor="c11", status={"type": "upToDate"}))
    _, _, states, _ = run(inline_config, catalog, states)
    assert requests_mock.last_request.json()["cursor"] == "c10"
    assert all("replay_from" not in shared(s) for s in states)


def test_read_restarts_full_refresh_when_rewinding_before_its_snapshot(requests_mock, inline_config, catalog):
    catalog.streams[1].sync_mode = SyncMode.full_refresh
    state = [
        stream_state(POSTS, "before-snapshot", checkpointed_at=1, snapshot_complete=True),
        stream_state(USER, "during-snapshot", checkpointed_at=2, snapshot_complete=False),
        stream_state(RATE_LIMITS, "during-snapshot", checkpointed_at=2, snapshot_complete=True),
    ]
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="primed")},
            {
                "json": page(
                    truncates=[USER_TRUNCATE],
                    values=[value("betterAuth", "user", {"_id": "unchanged-row"})],
                    cursor="done",
                    status={"type": "upToDate"},
                )
            },
        ],
    )
    _, records, _, _ = run(inline_config, catalog, state)
    first, second = [r.json() for r in requests_mock.request_history]
    assert first["cursor"] == "before-snapshot"
    assert "betterAuth" not in first["selection"]
    assert "betterAuth" in second["selection"]
    assert records[0].data["_id"] == "unchanged-row"


def test_read_preserves_rewind_point_when_ahead_stream_is_temporarily_deselected(requests_mock, inline_config, catalog):
    inline_config["max_pages_per_sync"] = 1
    ahead = stream_state(USER, "c9", checkpointed_at=2, snapshot_complete=True)
    state = [
        stream_state(POSTS, "c1", checkpointed_at=1, snapshot_complete=True),
        ahead,
        stream_state(RATE_LIMITS, "c9", checkpointed_at=2, snapshot_complete=True),
    ]
    requests_mock.post(SYNC_URL, json=page(cursor="c5"))
    _, _, states, _ = run(inline_config, catalog, state)
    # Only posts acknowledges c5; other streams are then deselected for a run.
    posts_state = next(s for s in states if ident(s.stream.stream_descriptor) == POSTS)
    original_streams = catalog.streams
    catalog.streams = original_streams[:1]
    requests_mock.post(SYNC_URL, json=page(cursor="c7"))
    _, _, states, _ = run(inline_config, catalog, [posts_state, ahead])
    assert requests_mock.last_request.json()["cursor"] == "c5"
    # Re-enabling user must not let its lower sequence number choose c9 ahead of posts/c7.
    catalog.streams = original_streams[:2]
    requests_mock.post(SYNC_URL, json=page(cursor="c10", status={"type": "upToDate"}))
    run(inline_config, catalog, [states[-1], ahead])
    assert requests_mock.last_request.json()["cursor"] == "c1"


@pytest.mark.parametrize("replaced", [False, True])
def test_full_refresh_emits_final_live_revisions_and_isolates_tables(requests_mock, inline_config, catalog, replaced):
    inline_config["state_checkpoint_pages"] = 1
    catalog.streams[0].sync_mode = SyncMode.full_refresh
    catalog.streams[1].sync_mode = SyncMode.full_refresh
    emitted = []
    pages = iter(
        [
            page(
                truncates=ALL_TRUNCATES,
                cursor="c1",
                values=[
                    value("", "posts", {"_id": "gone", "body": "same ID in a different table"}),
                    value("betterAuth", "user", {"_id": "gone", "name": "delete me"}),
                    value("betterAuth", "user", {"_id": "updated", "name": "old"}),
                    value("resend/rateLimiter", "rateLimits", {"_id": "incremental"}),
                ],
            ),
            page(
                cursor="c2",
                values=[
                    value("betterAuth", "user", {"_id": "gone"}, deleted=True),
                    value("betterAuth", "user", {"_id": "updated", "name": "new"}),
                ],
            ),
            page(
                cursor="done",
                status={"type": "upToDate"},
                truncates=[USER_TRUNCATE] if replaced else [],
                values=[value("betterAuth", "user", {"_id": "replacement"})] if replaced else [],
            ),
        ]
    )

    def respond(request, context):
        assert all(ident(m.record) == RATE_LIMITS for m in emitted if m.type == Type.RECORD)
        assert all(ident(m.state.stream.stream_descriptor) == RATE_LIMITS for m in emitted if m.type == Type.STATE)
        return next(pages)

    requests_mock.post(SYNC_URL, json=respond)
    emitted.extend(SourceConvexDataSync().read(logger, inline_config, catalog))
    records = [m.record for m in emitted if m.type == Type.RECORD]
    assert [r.data["_id"] for r in records if ident(r) == POSTS] == ["gone"]
    users = [r.data for r in records if ident(r) == USER]
    assert [r["_id"] for r in users] == (["replacement"] if replaced else ["updated"])
    if not replaced:
        assert users[0]["name"] == "new"
    assert users[0]["_ab_cdc_lsn"] == 1_788_466_011_116_811_508
    assert users[0]["_deleted"] is False
    last_record = max(i for i, m in enumerate(emitted) if m.type == Type.RECORD)
    assert all(
        i > last_record for i, m in enumerate(emitted) if m.type == Type.STATE and ident(m.state.stream.stream_descriptor) in (POSTS, USER)
    )


def test_failed_full_refresh_discards_buffer_and_rebuilds_on_retry(requests_mock, inline_config, catalog, monkeypatch, tmp_path):
    from functools import partial
    from tempfile import TemporaryDirectory

    monkeypatch.setattr("source_convex_data_sync.source.TemporaryDirectory", partial(TemporaryDirectory, dir=tmp_path))
    inline_config["state_checkpoint_pages"] = 1
    catalog.streams[1].sync_mode = SyncMode.full_refresh
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(truncates=ALL_TRUNCATES, cursor="c1", values=[value("betterAuth", "user", {"_id": "unchanged"})])},
            {"status_code": 403, "json": {"code": "Forbidden"}},
        ],
    )
    emitted = []
    with pytest.raises(AirbyteTracedException):
        emitted.extend(SourceConvexDataSync().read(logger, inline_config, catalog))
    assert list(tmp_path.iterdir()) == []
    assert not any(m.type == Type.RECORD for m in emitted)
    states = [m.state for m in emitted if m.type == Type.STATE]
    assert set(last_states(states)) == {POSTS, RATE_LIMITS}
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(cursor="primed")},
            {
                "json": page(
                    truncates=[USER_TRUNCATE],
                    cursor="done",
                    status={"type": "upToDate"},
                    values=[value("betterAuth", "user", {"_id": "unchanged"})],
                )
            },
        ],
    )
    _, records, states, _ = run(inline_config, catalog, states)
    assert [r.data["_id"] for r in records] == ["unchanged"]
    assert last_states(states)[USER]["cursor"] == "done"
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("snapshot_complete", [False, True])
def test_incremental_table_replacement_requires_reset_before_advancing(requests_mock, inline_config, catalog, snapshot_complete):
    state = [
        stream_state(key, "before-replacement", snapshot_complete=snapshot_complete, awaiting_truncate=False)
        for key in (POSTS, USER, RATE_LIMITS)
    ]
    requests_mock.post(
        SYNC_URL,
        json=page(
            truncates=[POSTS_TRUNCATE],
            cursor="after-replacement",
            values=[value("", "posts", {"_id": "replacement"})],
            status={"type": "upToDate"},
        ),
    )
    emitted = []
    with pytest.raises(AirbyteTracedException) as err:
        emitted.extend(SourceConvexDataSync().read(logger, inline_config, catalog, state))
    assert err.value.failure_type == FailureType.config_error
    assert "Clear this stream's data" in err.value.message
    assert not any(m.type == Type.RECORD for m in emitted)
    states = [m.state for m in emitted if m.type == Type.STATE]
    assert states == []
    assert shared(state[0])["cursor"] == "before-replacement"
    assert shared(state[0])["awaiting_truncate"] is False


def test_full_refresh_cancellation_closes_buffer_without_checkpoint(requests_mock, inline_config, catalog, monkeypatch, tmp_path):
    from functools import partial
    from tempfile import TemporaryDirectory

    monkeypatch.setattr("source_convex_data_sync.source.TemporaryDirectory", partial(TemporaryDirectory, dir=tmp_path))
    catalog.streams = catalog.streams[:1]
    catalog.streams[0].sync_mode = SyncMode.full_refresh
    inline_config["state_checkpoint_pages"] = 1
    requests_mock.post(
        SYNC_URL,
        json=page(
            truncates=[POSTS_TRUNCATE],
            status={"type": "upToDate"},
            values=[value("", "posts", {"_id": "a"}), value("", "posts", {"_id": "b"})],
        ),
    )
    reader = SourceConvexDataSync().read(logger, inline_config, catalog)
    for message in reader:
        assert message.type != Type.STATE
        if message.type == Type.RECORD:
            break
    assert list(tmp_path.iterdir())
    reader.close()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("resume", [False, True])
def test_repeated_truncate_announcements_before_any_rows_are_safe(requests_mock, inline_config, catalog, resume):
    inline_config["state_checkpoint_pages"] = 1
    if resume:
        inline_config["max_pages_per_sync"] = 1
    requests_mock.post(
        SYNC_URL,
        [
            {"json": page(truncates=ALL_TRUNCATES, cursor="announced")},
            {
                "json": page(
                    truncates=[POSTS_TRUNCATE],
                    cursor="done",
                    status={"type": "upToDate"},
                    values=[value("", "posts", {"_id": "first-row"})],
                )
            },
        ],
    )
    _, records, states, _ = run(inline_config, catalog)
    if resume:
        assert records == []
        assert last_states(states)[POSTS]["has_records"] is False
        _, records, states, _ = run(inline_config, catalog, states)
    assert [r.data["_id"] for r in records] == ["first-row"]
    assert last_states(states)[POSTS]["has_records"] is True


def test_failed_priming_page_does_not_cancel_a_cleared_stream_snapshot(requests_mock, inline_config, catalog):
    state = [stream_state(USER, "before-reset", has_records=True), stream_state(RATE_LIMITS, "before-reset", has_records=True)]
    requests_mock.post(SYNC_URL, json=page(truncates=[USER_TRUNCATE], cursor="primed"))
    emitted = []
    with pytest.raises(AirbyteTracedException):
        emitted.extend(SourceConvexDataSync().read(logger, inline_config, catalog, state))
    assert "" not in requests_mock.last_request.json()["selection"]
    # posts was cleared. Saving its new flags beside the unchanged old cursor
    # would make the retry skip the snapshot needed to repopulate it.
    assert not any(m.type == Type.STATE for m in emitted)
