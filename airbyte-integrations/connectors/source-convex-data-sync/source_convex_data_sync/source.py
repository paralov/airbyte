#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

"""Convex source built on the Deployment API data sync endpoint.

One Convex data sync (``POST /api/v1/data/sync``) streams every selected table
in the deployment, including tables that live inside installed components, and
returns a single opaque cursor per page. That maps onto Airbyte as:

* one Airbyte stream per ``(component, table)`` pair, named ``table`` for the
  root component and ``<component path>__<table>`` (``/`` becomes ``__``) for
  component tables;
* per-stream Airbyte state that each carry the same Convex cursor (the CDK
  entrypoint's record counting requires per-stream state), plus a little
  per-stream bookkeeping;
* the configured catalog driving the Convex ``selection`` so the deployment only
  ships tables Airbyte asked for.

See https://docs.convex.dev/deployment-api/data-sync.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple

import requests

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    AirbyteStreamStatus,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    StreamDescriptor,
    SyncMode,
    Type,
)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.utils.record_helper import stream_data_to_airbyte_message
from airbyte_cdk.utils.stream_status_utils import as_airbyte_message as stream_status_message
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType


LOGGER = logging.getLogger("airbyte")

CONVEX_CLIENT_VERSION = "0.1.0"
CONVEX_CLIENT_HEADER = f"airbyte-data-sync-{CONVEX_CLIENT_VERSION}"

ROOT_COMPONENT = ""
COMPONENT_SEPARATOR = "__"

# The documented ``status.type`` values are ``snapshotting``, ``stale`` and ``upToDate``.
# ``pagination.hasMore`` is documented as always true, so ``upToDate`` is the only completion signal.
UP_TO_DATE_STATUS = "upToDate"

# The sync went unused for more than 3 days: Convex forgot it, so restart from scratch.
RESTART_ERROR_CODES = {"DataSyncCursorExpired"}
# The saved cursor does not belong to this deployment (for example deployment_url was
# changed). Restarting would layer a fresh snapshot on top of the old rows, so refuse.
FOREIGN_CURSOR_ERROR_CODES = {"InvalidDataSyncCursor"}

# Responses that mean the request itself is wrong (bad URL, key, plan or selection) rather than transient.
CONFIG_ERROR_STATUS_CODES = {400, 401, 403, 404}

# Raised by ``requests`` before any network I/O: retrying cannot help and the deployment URL is wrong.
NON_RETRYABLE_REQUEST_ERRORS = (
    requests.exceptions.InvalidSchema,
    requests.exceptions.InvalidURL,
    requests.exceptions.MissingSchema,
)

DEFAULT_REQUEST_TIMEOUT_SECONDS = 120
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 30
MAX_RETRY_AFTER_SECONDS = 60
STATE_CHECKPOINT_PAGES = 25

SYSTEM_FIELD_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "_id": {"type": "string"},
    "_creationTime": {"type": "number"},
    "_ts": {"type": "integer"},
    "_deleted": {"type": "boolean"},
    "_component": {"type": "string"},
    "_table": {"type": "string"},
    "_ab_cdc_lsn": {"type": "number"},
    "_ab_cdc_updated_at": {"type": "string"},
    "_ab_cdc_deleted_at": {"anyOf": [{"type": "string"}, {"type": "null"}]},
}


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------


def stream_name_for(component: str, table: str) -> str:
    """``users`` for root tables, ``betterAuth__user`` / ``resend__emailWorkpool__payload`` for component tables.

    Convex identifiers may themselves contain ``__``, so this mapping is not injective; ``SourceConvex.streams``
    rejects deployments where two tables collide, and ``read`` never parses names back.
    """
    if component == ROOT_COMPONENT:
        return table
    return f"{component.replace('/', COMPONENT_SEPARATOR)}{COMPONENT_SEPARATOR}{table}"


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class ConvexApiError(Exception):
    def __init__(self, context: str, status_code: int, code: Optional[str], message: str, failure_type: Optional[FailureType] = None):
        super().__init__(f"{context}: {status_code}: {code or 'error'}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message
        self._failure_type = failure_type

    @property
    def failure_type(self) -> FailureType:
        if self._failure_type is not None:
            return self._failure_type
        return FailureType.config_error if self.status_code in CONFIG_ERROR_STATUS_CODES else FailureType.system_error

    def as_traced(self) -> AirbyteTracedException:
        return AirbyteTracedException(message=str(self), failure_type=self.failure_type)


def _parse_error(resp: requests.Response) -> Tuple[Optional[str], str]:
    try:
        err = resp.json()
    except ValueError:
        return None, resp.text
    if isinstance(err, dict):
        return err.get("code"), str(err.get("message", resp.text))
    return None, resp.text


def _retry_delay(resp: requests.Response, attempt: int) -> float:
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), MAX_RETRY_AFTER_SECONDS)
        except ValueError:
            pass  # HTTP-date form; fall back to exponential backoff.
    return min(2**attempt, MAX_BACKOFF_SECONDS)


class ConvexClient:
    """Thin client over the deployment endpoints this connector needs."""

    def __init__(
        self,
        deployment_url: str,
        access_key: str,
        timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        session: Optional[requests.Session] = None,
    ):
        self.deployment_url = deployment_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Convex {access_key}",
                "Convex-Client": CONVEX_CLIENT_HEADER,
            }
        )

    def _request(
        self,
        method: str,
        path: str,
        context: str,
        json_body: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        url = f"{self.deployment_url}{path}"
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self.session.request(method, url, json=json_body, params=params, timeout=self.timeout)
            except NON_RETRYABLE_REQUEST_ERRORS as e:
                raise ConvexApiError(context, 0, "InvalidDeploymentUrl", str(e), failure_type=FailureType.config_error) from e
            except requests.RequestException as e:
                if attempt >= MAX_ATTEMPTS:
                    raise ConvexApiError(context, 0, "RequestException", str(e)) from e
                delay = min(2**attempt, MAX_BACKOFF_SECONDS)
                LOGGER.info("%s: %s; retrying in %.0fs (attempt %d/%d)", context, e, delay, attempt, MAX_ATTEMPTS)
                time.sleep(delay)
                continue
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as e:
                    raise ConvexApiError(context, 200, "InvalidJSON", resp.text[:200]) from e
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_ATTEMPTS:
                delay = _retry_delay(resp, attempt)
                LOGGER.info("%s: HTTP %d; retrying in %.0fs (attempt %d/%d)", context, resp.status_code, delay, attempt, MAX_ATTEMPTS)
                time.sleep(delay)
                continue
            code, message = _parse_error(resp)
            raise ConvexApiError(context, resp.status_code, code, message)

    def list_active_syncs(self) -> Mapping[str, Any]:
        return self._request("GET", "/api/v1/data/list_active_syncs", "Listing active data syncs", params={"limit": 1})

    def data_sync(self, cursor: Optional[str], selection: Optional[Mapping[str, Any]]) -> Mapping[str, Any]:
        body: Dict[str, Any] = {}
        if cursor is not None:
            body["cursor"] = cursor
        if selection is not None:
            body["selection"] = selection
        return self._request("POST", "/api/v1/data/sync", "Data sync page", json_body=body)

    def json_schemas(self) -> Mapping[str, Any]:
        """``{component path: {table: schema}}`` describing values the way ``/api/v1/data/sync`` encodes them."""
        return self._request(
            "GET",
            "/api/json_schemas",
            "Fetching table schemas",
            # byComponent groups tables under their component path ("" for the root); export_json matches the
            # lossless encoding the data sync endpoint always uses (int64 as {"$integer": ...} and so on).
            params={"deltaSchema": "true", "format": "export_json", "byComponent": "true"},
        )


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


TableSchemas = Dict[Tuple[str, str], Dict[str, Any]]  # (component, table) -> JSON schema


def _looks_like_bare_schema(value: Mapping[str, Any]) -> bool:
    """A JSON Schema has string-valued ``type``/``$schema`` keywords; a ``{table: schema}`` map only has dict values."""
    return isinstance(value.get("type"), (str, list)) or isinstance(value.get("$schema"), str)


def parse_inline_schema(schema_json: str) -> TableSchemas:
    """Parses the ``{"<component path>": {"<table>": <JSON Schema>}}`` form, with ``""`` for the root component."""
    try:
        parsed = json.loads(schema_json)
    except ValueError as e:
        raise AirbyteTracedException(
            message="The inline schema is not valid JSON.",
            internal_message=str(e),
            failure_type=FailureType.config_error,
        ) from e
    if not isinstance(parsed, dict):
        raise AirbyteTracedException(message="The inline schema must be a JSON object.", failure_type=FailureType.config_error)
    out: TableSchemas = {}
    for component, tables in parsed.items():
        if not isinstance(tables, dict):
            raise AirbyteTracedException(
                message=f"Inline schema entry {component!r} must map table names to JSON Schemas.",
                failure_type=FailureType.config_error,
            )
        if _looks_like_bare_schema(tables):
            raise AirbyteTracedException(
                message=(
                    f"Inline schema entry {component!r} looks like a table schema. Wrap root tables under the empty "
                    f'component path: {{"": {{"{component}": <JSON Schema>}}}}.'
                ),
                failure_type=FailureType.config_error,
            )
        for table, schema in tables.items():
            if not isinstance(schema, dict):
                raise AirbyteTracedException(
                    message=f"Schema for table {component!r}/{table!r} must be a JSON Schema object.",
                    failure_type=FailureType.config_error,
                )
            out[(component, table)] = schema
    return out


def schemas_from_api(client: ConvexClient) -> TableSchemas:
    return {(component, table): schema for component, tables in client.json_schemas().items() for table, schema in tables.items()}


def airbyte_schema_for(table_schema: Mapping[str, Any]) -> Dict[str, Any]:
    """Copy the table schema, add system + CDC fields, and relax ``additionalProperties``."""
    schema: Dict[str, Any] = {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object"}
    branches = table_schema.get("anyOf")
    if isinstance(branches, list):
        # Union document types: merge every branch's properties into one permissive schema.
        properties: Dict[str, Any] = {}
        for branch in branches:
            if isinstance(branch, dict):
                properties.update(branch.get("properties", {}))
    else:
        properties = dict(table_schema.get("properties", {}))
    properties.update(SYSTEM_FIELD_SCHEMAS)
    schema["properties"] = properties
    schema["additionalProperties"] = True
    return schema


def with_convex_origin(schema: Dict[str, Any], component: str, table: str) -> Dict[str, Any]:
    """Record the (component, table) origin in the stream schema so read() never has to parse stream names."""
    schema["x-convex-component"] = component
    schema["x-convex-table"] = table
    return schema


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------


class ConvexTableStream(Stream):
    primary_key = "_id"
    cursor_field = "_ts"

    def __init__(self, component: str, table: str, json_schema: Mapping[str, Any]):
        self.component = component
        self.table = table
        self._json_schema = with_convex_origin(airbyte_schema_for(json_schema), component, table)

    @property
    def name(self) -> str:
        return stream_name_for(self.component, self.table)

    def get_json_schema(self) -> Mapping[str, Any]:
        return self._json_schema

    def read_records(self, *args: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        raise NotImplementedError("Convex streams are read together by SourceConvexDataSync.read via the data sync endpoint.")


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def _blob_to_dict(blob: Any) -> Dict[str, Any]:
    # AirbyteStateBlob is a plain attribute bag; the entrypoint always hands us that type (or None).
    return {} if blob is None else dict(vars(blob))


class SyncState:
    """Convex sync state.

    The Convex cursor covers every table, but the Airbyte CDK entrypoint requires
    per-stream state messages (``message_utils.get_stream_descriptor`` rejects
    GLOBAL state), so the same cursor is written into every stream's state at each
    checkpoint along with a ``checkpointed_at`` stamp. On resume the most recently
    checkpointed cursor wins, and only streams that took part in that checkpoint
    keep their per-stream bookkeeping: a stream whose state is older (it was
    deselected for a while, or its checkpoint was not persisted) is re-synced from
    scratch, just like one whose state was cleared.
    """

    SHARED_KEYS = ("cursor", "sync_id", "selection_hash", "checkpointed_at")

    def __init__(self) -> None:
        self.cursor: Optional[str] = None
        self.sync_id: Optional[str] = None
        self.selection_hash: Optional[str] = None
        self.streams: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def _stream_keys(cls, blob: Mapping[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in blob.items() if k not in cls.SHARED_KEYS}

    @classmethod
    def from_messages(cls, logger: logging.Logger, state: Optional[List[AirbyteStateMessage]]) -> "SyncState":
        out = cls()
        if not state:
            return out
        blobs: Dict[str, Dict[str, Any]] = {}
        for message in state:
            if message.type == AirbyteStateType.STREAM and message.stream is not None:
                blob = _blob_to_dict(message.stream.stream_state)
                if "cursor" in blob:
                    blobs[message.stream.stream_descriptor.name] = blob
        if not blobs:
            logger.warning("No resumable Convex cursor in the incoming state; starting a fresh data sync.")
            return out
        latest = max(blobs.values(), key=lambda c: c.get("checkpointed_at") or 0)
        out.cursor = latest.get("cursor")
        out.sync_id = latest.get("sync_id")
        out.selection_hash = latest.get("selection_hash")
        out.streams = {
            name: cls._stream_keys(blob) for name, blob in blobs.items() if blob.get("checkpointed_at") == latest.get("checkpointed_at")
        }
        return out

    def restart(self) -> None:
        """Forget the cursor and every per-stream flag: the next page starts a brand new data sync."""
        self.cursor = None
        self.sync_id = None
        self.streams = {}

    def stream(self, name: str) -> Dict[str, Any]:
        return self.streams.setdefault(name, {})

    def to_messages(self, stream_names: Iterable[str]) -> Iterator[AirbyteMessage]:
        checkpointed_at = int(time.time() * 1000)
        for name in stream_names:
            blob = AirbyteStateBlob(
                cursor=self.cursor,
                sync_id=self.sync_id,
                selection_hash=self.selection_hash,
                checkpointed_at=checkpointed_at,
                **self.streams.get(name, {}),
            )
            yield AirbyteMessage(
                type=Type.STATE,
                state=AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(stream_descriptor=StreamDescriptor(name=name), stream_state=blob),
                ),
            )


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


def build_selection(pairs: Iterable[Tuple[str, str]]) -> Dict[str, Any]:
    """Convex ``selection`` body that includes exactly the given ``(component, table)`` pairs."""
    selection: Dict[str, Any] = {"_other": "excluded"}
    for component, table in pairs:
        component_selection = selection.setdefault(component, {"_other": "excluded"})
        component_selection[table] = {"_other": "included"}
    return selection


def _selection_hash(selection: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(selection, sort_keys=True).encode()).hexdigest()[:16]


def _record_from_value(value: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(value["value"])
    ts_ns = int(value["ts"])
    deleted = bool(value.get("deleted", False))
    # Naive UTC ISO string, matching the format used by the original Convex source connector.
    ts_iso = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).replace(tzinfo=None).isoformat()
    record["_ts"] = ts_ns
    record["_deleted"] = deleted
    record["_component"] = value["component"]
    record["_table"] = value["table"]
    # Same CDC columns as Debezium-based sources so destinations dedupe and delete consistently.
    record["_ab_cdc_lsn"] = ts_ns
    record["_ab_cdc_updated_at"] = ts_iso
    record["_ab_cdc_deleted_at"] = ts_iso if deleted else None
    return record


class SourceConvexDataSync(AbstractSource):
    def _client(self, config: Mapping[str, Any]) -> ConvexClient:
        return ConvexClient(
            config["deployment_url"],
            config["access_key"],
            timeout=int(config.get("request_timeout_seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS)),
        )

    # -- spec helpers -----------------------------------------------------

    @staticmethod
    def _schema_source(config: Mapping[str, Any]) -> Mapping[str, Any]:
        return config.get("schema_source") or {"type": "api"}

    def _table_schemas(self, config: Mapping[str, Any], client: ConvexClient) -> TableSchemas:
        source = self._schema_source(config)
        kind = source.get("type", "api")
        if kind == "inline":
            return parse_inline_schema(source.get("schema_json") or "")
        if kind == "api":
            return schemas_from_api(client)
        raise AirbyteTracedException(message=f"Unknown schema_source type {kind!r}.", failure_type=FailureType.config_error)

    # -- check / discover -------------------------------------------------

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        client = self._client(config)
        try:
            client.list_active_syncs()
            streams = self._streams(config, client)
        except AirbyteTracedException as e:
            return False, e.message
        except ConvexApiError as e:
            return False, str(e)
        if not streams:
            return False, "No tables found. Check the deployment URL, the deploy key, and the schema source."
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return self._streams(config, self._client(config))

    def _streams(self, config: Mapping[str, Any], client: ConvexClient) -> List[ConvexTableStream]:
        try:
            schemas = self._table_schemas(config, client)
        except ConvexApiError as e:
            raise e.as_traced() from e
        streams = [ConvexTableStream(component, table, schema) for (component, table), schema in sorted(schemas.items())]
        seen: Dict[str, ConvexTableStream] = {}
        for stream in streams:
            other = seen.setdefault(stream.name, stream)
            if other is not stream:
                raise AirbyteTracedException(
                    message=(
                        f"Tables {other.component or '<root>'}/{other.table} and {stream.component or '<root>'}/{stream.table} "
                        f"both map to the Airbyte stream name {stream.name!r}. Rename one of them or exclude it with an inline schema."
                    ),
                    failure_type=FailureType.config_error,
                )
        return streams

    # -- read -------------------------------------------------------------

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ) -> Iterator[AirbyteMessage]:
        client = self._client(config)
        configured = {cs.stream.name: cs for cs in catalog.streams}
        if not configured:
            logger.info("No streams selected; nothing to sync.")
            return
        sync_state = SyncState.from_messages(logger, state)
        max_pages = int(config.get("max_pages_per_sync") or 0)
        checkpoint_pages = int(config.get("state_checkpoint_pages") or STATE_CHECKPOINT_PAGES)

        # Resolve every configured stream to its (component, table) against what the deployment exposes now.
        known = self._known_streams(logger, config, client, configured)
        pair_to_name: Dict[Tuple[str, str], str] = {}
        for name, cs in configured.items():
            pair = self._pair_for(cs, known)
            if pair is None:
                logger.warning(
                    "The stream %r in your connection configuration was not found in the source. Refresh the schema in your "
                    "connection or inline schema to remove it.",
                    name,
                )
                yield stream_status_message(cs.stream, AirbyteStreamStatus.INCOMPLETE)
                continue
            pair_to_name[pair] = name
        selected = {name: configured[name] for name in pair_to_name.values()}
        if not selected:
            return

        full_selection = build_selection(pair_to_name.keys())
        full_hash = _selection_hash(full_selection)

        resuming = sync_state.cursor is not None
        if resuming and sync_state.selection_hash and sync_state.selection_hash != full_hash:
            logger.info("Stream selection changed since the last sync; Convex will sync newly selected tables from scratch.")

        for cs in selected.values():
            yield stream_status_message(cs.stream, AirbyteStreamStatus.STARTED)

        # Streams that must be re-sent from scratch on a resumed sync: full refresh streams (unless a previous
        # run's re-sync of them is still in progress, in which case Convex just carries on), and streams whose
        # own state was cleared (the platform's per-stream reset), is stale, or that were never synced before.
        # Deselecting them for one page and reselecting them makes Convex re-sync them from scratch with a
        # leading truncate (documented ``selection`` semantics).
        full_refresh = {name for name, cs in selected.items() if cs.sync_mode == SyncMode.full_refresh}
        priming_selection: Optional[Dict[str, Any]] = None
        if resuming:
            resnapshot = {
                name
                for name in selected
                if name not in sync_state.streams
                or (name in full_refresh and sync_state.streams[name].get("snapshot_complete") is not False)
            }
            if resnapshot:
                keep = [pair for pair, name in pair_to_name.items() if name not in resnapshot]
                priming_selection = build_selection(keep)
                for name in resnapshot:
                    sync_state.stream(name)["snapshot_complete"] = False
                logger.info("Asking Convex to re-sync %s from scratch.", sorted(resnapshot))
        for name in selected:
            sync_state.stream(name).setdefault("snapshot_complete", False)

        def finish(status: AirbyteStreamStatus, incomplete: Set[str] = frozenset()) -> Iterator[AirbyteMessage]:
            yield from sync_state.to_messages(selected.keys())
            for name, cs in selected.items():
                yield stream_status_message(cs.stream, AirbyteStreamStatus.INCOMPLETE if name in incomplete else status)

        running_emitted: Set[str] = set()
        dropped: Dict[Tuple[str, str], int] = {}
        pages = 0
        records = 0
        restarted = False
        status_type: Optional[str] = None

        try:
            while True:
                priming = priming_selection is not None
                selection = priming_selection if priming else full_selection
                try:
                    page = client.data_sync(sync_state.cursor, selection)
                except ConvexApiError as e:
                    if e.code in FOREIGN_CURSOR_ERROR_CODES and sync_state.cursor is not None:
                        raise AirbyteTracedException(
                            message=(
                                "Convex rejected the saved sync cursor. This usually means the deployment URL changed since the "
                                "last sync. Clear the connection's data so the new deployment is synced from scratch."
                            ),
                            internal_message=str(e),
                            failure_type=FailureType.config_error,
                        ) from e
                    if e.code in RESTART_ERROR_CODES and sync_state.cursor is not None and not restarted:
                        logger.warning(
                            "Convex rejected the saved cursor (%s); restarting the data sync from scratch. Every table will be re-sent "
                            "in full, but rows deleted since the last sync are not tombstoned, so reset the streams in the destination "
                            "if you need an exact mirror.",
                            e.code,
                        )
                        sync_state.restart()
                        priming_selection = None
                        restarted = True
                        continue
                    raise e.as_traced() from e
                priming_selection = None
                pages += 1

                sync_state.sync_id = page.get("syncId", sync_state.sync_id)
                sync_state.selection_hash = full_hash

                for truncate in page.get("truncates", []):
                    pair = (truncate.get("component", ROOT_COMPONENT), truncate["table"])
                    name = pair_to_name.get(pair)
                    if name is None:
                        continue
                    stream_state = sync_state.stream(name)
                    if stream_state.get("snapshot_complete"):
                        # Truncates during the initial snapshot are normal; after it they mean the table was replaced.
                        logger.warning(
                            "Convex truncated table %s (component %r); it will be re-sent in full. Rows deleted before the truncate "
                            "are not tombstoned, so reset this stream in the destination if you need an exact mirror.",
                            truncate["table"],
                            pair[0],
                        )
                    stream_state["snapshot_complete"] = False

                for value in page.get("values", []):
                    pair = (value.get("component", ROOT_COMPONENT), value["table"])
                    name = pair_to_name.get(pair)
                    if name is None:
                        dropped[pair] = dropped.get(pair, 0) + 1
                        continue
                    if name in full_refresh and value.get("deleted"):
                        # A full refresh mirrors the live documents; a tombstone would land as a row of nulls.
                        continue
                    if name not in running_emitted:
                        running_emitted.add(name)
                        yield stream_status_message(selected[name].stream, AirbyteStreamStatus.RUNNING)
                    records += 1
                    yield stream_data_to_airbyte_message(name, _record_from_value(value))

                sync_state.cursor = page["pagination"]["nextCursor"]
                status_type = page.get("status", {}).get("type")
                up_to_date = status_type == UP_TO_DATE_STATUS
                # The priming page describes a sync that still excludes the re-snapshot streams, so it never
                # counts as complete for them.
                if up_to_date and not priming:
                    for name in selected:
                        sync_state.stream(name)["snapshot_complete"] = True

                if pages % checkpoint_pages == 0:
                    yield from sync_state.to_messages(selected.keys())

                if priming:
                    continue  # Always fetch at least one page with the full selection so the deselected tables come back.
                # An upToDate page is a consistent snapshot through its snapshotTs. `hasMore` is always true on a
                # live sync and a busy deployment can keep upToDate pages non-empty forever, so stop here.
                if up_to_date:
                    break
                if max_pages and pages >= max_pages:
                    logger.info(
                        "Reached max_pages_per_sync=%d; checkpointing and stopping. The next sync resumes from the saved cursor.", max_pages
                    )
                    break
        except Exception:
            yield from finish(AirbyteStreamStatus.INCOMPLETE)
            raise

        # A full refresh stream is only complete once the sync is up to date; stopping early leaves its snapshot partial.
        partial = {name for name in full_refresh if not sync_state.stream(name).get("snapshot_complete")}
        if partial:
            logger.warning(
                "Stopped before the full refresh snapshot of %s finished; reporting them incomplete so the destination keeps "
                "its previous data. Raise max_pages_per_sync or switch them to incremental.",
                sorted(partial),
            )
        yield from finish(AirbyteStreamStatus.COMPLETE, incomplete=partial)
        if dropped:
            logger.warning(
                "Dropped %d documents from tables not in the configured catalog: %s",
                sum(dropped.values()),
                ", ".join(f"{c or '<root>'}/{t}={n}" for (c, t), n in sorted(dropped.items())),
            )
        logger.info("Convex data sync %s: %d pages, %d records, status=%s", sync_state.sync_id, pages, records, status_type or "none")

    def _known_streams(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        client: ConvexClient,
        configured: Mapping[str, ConfiguredAirbyteStream],
    ) -> Dict[str, Tuple[str, str]]:
        """Stream name -> (component, table) as the deployment exposes them now.

        Schema discovery is a hard dependency of discover, but a read should not fail because one unrelated
        table cannot be described. When every configured stream carries the origin hint discover wrote, fall
        back to those hints and let Convex validate the selection.
        """
        try:
            return {stream.name: (stream.component, stream.table) for stream in self._streams(config, client)}
        except AirbyteTracedException as e:
            hinted: Dict[str, Tuple[str, str]] = {}
            for name, cs in configured.items():
                schema = cs.stream.json_schema or {}
                if "x-convex-table" not in schema:
                    raise
                hinted[name] = (schema.get("x-convex-component", ROOT_COMPONENT), schema["x-convex-table"])
            logger.warning("Could not fetch table schemas (%s); syncing the configured streams from their saved schemas.", e.message)
            return hinted

    @staticmethod
    def _pair_for(configured_stream: ConfiguredAirbyteStream, known: Mapping[str, Tuple[str, str]]) -> Optional[Tuple[str, str]]:
        """``(component, table)`` of the discovered stream with this name, or None when the deployment no longer has it.

        The schema's origin hint (written by discover) must agree: if a different table now maps to this stream name,
        the configured stream is treated as missing rather than silently bound to the new table.
        """
        pair = known.get(configured_stream.stream.name)
        schema = configured_stream.stream.json_schema or {}
        if pair is not None and "x-convex-table" in schema:
            hint = (schema.get("x-convex-component", ROOT_COMPONENT), schema["x-convex-table"])
            if hint != pair:
                return None
        return pair
