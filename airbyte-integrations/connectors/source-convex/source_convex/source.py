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
  requires per-stream state), plus a little per-stream bookkeeping;
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
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

import requests

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
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
from airbyte_cdk.utils.stream_status_utils import as_airbyte_message as stream_status_message
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType


CONVEX_CLIENT_VERSION = "1.0.0"
CONVEX_CLIENT_HEADER = f"airbyte-export-{CONVEX_CLIENT_VERSION}"

ROOT_COMPONENT = ""
COMPONENT_SEPARATOR = "__"

# Status values observed from the endpoint. The published OpenAPI types say
# ``inProgress`` / ``synced``; the live API says ``snapshotting`` / ``stale`` /
# ``upToDate``. Treat both vocabularies as equivalent.
UP_TO_DATE_STATUSES = {"upToDate", "synced"}

# Error codes that mean the cursor is unusable and the sync must restart.
RESTART_ERROR_CODES = {"DataSyncCursorExpired", "InvalidDataSyncCursor"}

DEFAULT_REQUEST_TIMEOUT_SECONDS = 120
MAX_RETRIES = 5
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
    """``users`` for root tables, ``betterAuth__user`` / ``resend__emailWorkpool__payload`` for component tables."""
    if component == ROOT_COMPONENT:
        return table
    return f"{component.replace('/', COMPONENT_SEPARATOR)}{COMPONENT_SEPARATOR}{table}"


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class ConvexApiError(Exception):
    def __init__(self, context: str, status_code: int, code: Optional[str], message: str):
        super().__init__(f"{context}: {status_code}: {code or 'error'}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message


def _parse_error(resp: requests.Response) -> Tuple[Optional[str], str]:
    try:
        err = resp.json()
        return err.get("code"), err.get("message", resp.text)
    except ValueError:
        return None, resp.text


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
            except requests.RequestException as e:
                if attempt >= MAX_RETRIES:
                    raise ConvexApiError(context, 0, "RequestException", str(e)) from e
                time.sleep(min(2**attempt, 30))
                continue
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(2**attempt, 30)
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
        return self._request(
            "GET",
            "/api/json_schemas",
            "Fetching table schemas",
            params={"deltaSchema": "true", "format": "json"},
        )


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


TableSchemas = Dict[Tuple[str, str], Dict[str, Any]]  # (component, table) -> JSON schema


def _looks_like_json_schema(value: Any) -> bool:
    return isinstance(value, dict) and ("properties" in value or "type" in value or "anyOf" in value or "$schema" in value)


def parse_inline_schema(schema_json: str) -> TableSchemas:
    """Accepts ``{component: {table: schema}}`` or the flat ``{table: schema}`` form (root only)."""
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
    for key, value in parsed.items():
        if _looks_like_json_schema(value):
            out[(ROOT_COMPONENT, key)] = value
        elif isinstance(value, dict):
            for table, schema in value.items():
                if not isinstance(schema, dict):
                    raise AirbyteTracedException(
                        message=f"Schema for table {key!r}/{table!r} must be a JSON Schema object.",
                        failure_type=FailureType.config_error,
                    )
                out[(key, table)] = schema
        else:
            raise AirbyteTracedException(
                message=f"Unexpected value for key {key!r} in inline schema.",
                failure_type=FailureType.config_error,
            )
    return out


def schemas_from_api(client: ConvexClient) -> TableSchemas:
    """``/api/json_schemas`` is keyed by bare table name and cannot distinguish components; treat everything as root."""
    return {(ROOT_COMPONENT, table): schema for table, schema in client.json_schemas().items()}


def airbyte_schema_for(table_schema: Mapping[str, Any]) -> Dict[str, Any]:
    """Copy the table schema, add system + CDC fields, and relax ``additionalProperties``."""
    schema: Dict[str, Any] = {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object"}
    if "anyOf" in table_schema:
        # Union document types: merge every branch's properties into one permissive schema.
        properties: Dict[str, Any] = {}
        for branch in table_schema["anyOf"]:
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

    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def source_defined_cursor(self) -> bool:
        return True

    def get_json_schema(self) -> Mapping[str, Any]:
        return self._json_schema

    def read_records(self, *args: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        raise NotImplementedError("Convex streams are read together by SourceConvex.read via the data sync endpoint.")


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def _blob_to_dict(blob: Any) -> Dict[str, Any]:
    if blob is None:
        return {}
    if isinstance(blob, dict):
        return dict(blob)
    for attr in ("model_dump", "dict"):
        fn = getattr(blob, attr, None)
        if callable(fn):
            try:
                return dict(fn())
            except TypeError:
                pass
    return {k: v for k, v in vars(blob).items() if not k.startswith("_")}


class SyncState:
    """Convex sync state.

    The Convex cursor covers every table, but the Airbyte CDK entrypoint requires
    per-stream state messages, so the same cursor is written into every stream's
    state at each checkpoint along with a ``checkpointed_at`` stamp. On resume the
    most recently checkpointed cursor wins.
    """

    SHARED_KEYS = ("cursor", "sync_id", "selection_hash", "checkpointed_at")

    def __init__(self) -> None:
        self.cursor: Optional[str] = None
        self.sync_id: Optional[str] = None
        self.selection_hash: Optional[str] = None
        self.streams: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def from_messages(cls, logger: logging.Logger, state: Optional[List[AirbyteStateMessage]]) -> "SyncState":
        out = cls()
        if not state:
            return out
        candidates: List[Dict[str, Any]] = []
        for message in state:
            if message.type == AirbyteStateType.STREAM and message.stream is not None:
                name = message.stream.stream_descriptor.name
                blob = _blob_to_dict(message.stream.stream_state)
                if "cursor" in blob:
                    candidates.append(blob)
                    out.streams[name] = {k: v for k, v in blob.items() if k not in cls.SHARED_KEYS}
            elif message.type == AirbyteStateType.GLOBAL and message.global_ is not None:
                shared = _blob_to_dict(message.global_.shared_state)
                if "cursor" in shared:
                    candidates.append(shared)
                for stream_state in message.global_.stream_states or []:
                    out.streams[stream_state.stream_descriptor.name] = _blob_to_dict(stream_state.stream_state)
        if not candidates:
            logger.warning(
                "No resumable Convex cursor in the incoming state (state from source-convex < 1.0.0 is not resumable); starting a fresh data sync."
            )
            return out
        latest = max(candidates, key=lambda c: c.get("checkpointed_at") or 0)
        out.cursor = latest.get("cursor")
        out.sync_id = latest.get("sync_id")
        out.selection_hash = latest.get("selection_hash")
        return out

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
    selection: Dict[str, Any] = {"_other": "excl"}
    for component, table in pairs:
        component_selection = selection.setdefault(component, {"_other": "excl"})
        component_selection[table] = {"_other": "incl"}
    return selection


def _selection_hash(selection: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(selection, sort_keys=True).encode()).hexdigest()[:16]


def _record_from_value(value: Mapping[str, Any]) -> Dict[str, Any]:
    record = dict(value["value"])
    ts_ns = int(value["ts"])
    deleted = bool(value.get("deleted", False))
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


class SourceConvex(AbstractSource):
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
        except ConvexApiError as e:
            return False, str(e)
        try:
            schemas = self._table_schemas(config, client)
        except AirbyteTracedException as e:
            return False, e.message
        except ConvexApiError as e:
            return False, str(e)
        if not schemas:
            return False, "No tables found. Check the deployment URL, the deploy key, and the schema source."
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        client = self._client(config)
        try:
            schemas = self._table_schemas(config, client)
        except ConvexApiError as e:
            raise AirbyteTracedException(message=str(e), failure_type=FailureType.config_error) from e
        return [ConvexTableStream(component, table, schema) for (component, table), schema in sorted(schemas.items())]

    # -- read -------------------------------------------------------------

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ) -> Iterator[AirbyteMessage]:
        client = self._client(config)
        configured = self._configured_streams(catalog)
        if not configured:
            logger.info("No streams selected; nothing to sync.")
            return
        sync_state = SyncState.from_messages(logger, state)
        max_pages = int(config.get("max_pages_per_sync") or 0)
        checkpoint_pages = int(config.get("state_checkpoint_pages") or STATE_CHECKPOINT_PAGES)

        by_name = configured
        pair_to_name = {self._pair_for(cs): name for name, cs in configured.items()}
        full_selection = build_selection(pair_to_name.keys())
        full_hash = _selection_hash(full_selection)

        full_refresh_names = {name for name, cs in configured.items() if cs.sync_mode == SyncMode.full_refresh}
        resuming = sync_state.cursor is not None
        if resuming and sync_state.selection_hash and sync_state.selection_hash != full_hash:
            logger.info("Stream selection changed since the last sync; Convex will sync newly selected tables from scratch.")

        for name in configured:
            yield stream_status_message(configured[name].stream, AirbyteStreamStatus.STARTED)

        # Full refresh streams on a resumed sync: deselect them for one page so Convex
        # re-syncs them from scratch when they are selected again.
        priming_selection: Optional[Dict[str, Any]] = None
        if resuming and full_refresh_names:
            keep = [pair for pair, name in pair_to_name.items() if name not in full_refresh_names]
            priming_selection = build_selection(keep)
            logger.info("Full refresh requested for %s; asking Convex to re-sync them from scratch.", sorted(full_refresh_names))

        running_emitted: set = set()
        dropped: Dict[Tuple[str, str], int] = {}
        pages = 0
        records = 0
        restarted = False

        try:
            while True:
                selection = priming_selection if priming_selection is not None else full_selection
                try:
                    page = client.data_sync(sync_state.cursor, selection)
                except ConvexApiError as e:
                    if e.code in RESTART_ERROR_CODES and sync_state.cursor is not None and not restarted:
                        logger.warning("Convex rejected the saved cursor (%s); restarting the data sync from scratch.", e.code)
                        sync_state.cursor = None
                        sync_state.sync_id = None
                        priming_selection = None
                        restarted = True
                        continue
                    raise AirbyteTracedException(message=str(e), failure_type=FailureType.system_error) from e
                priming_selection = None
                pages += 1

                sync_state.sync_id = page.get("syncId", sync_state.sync_id)
                sync_state.selection_hash = full_hash

                for truncate in page.get("truncates", []):
                    pair = (truncate.get("component", ROOT_COMPONENT), truncate["table"])
                    name = pair_to_name.get(pair)
                    if name is None:
                        continue
                    stream_state = sync_state.streams.setdefault(name, {})
                    if stream_state.get("snapshot_complete"):
                        # Truncates during the initial snapshot are normal; after it they mean the table was replaced.
                        logger.warning(
                            "Convex truncated table %s (component %r); it will be re-sent in full. Rows deleted before the truncate "
                            "are not tombstoned, so reset this stream in the destination if you need an exact mirror.",
                            truncate["table"],
                            pair[0],
                        )
                        stream_state["truncated_at"] = int(time.time() * 1000)
                    stream_state["snapshot_complete"] = False

                for value in page.get("values", []):
                    pair = (value.get("component", ROOT_COMPONENT), value["table"])
                    name = pair_to_name.get(pair)
                    if name is None:
                        dropped[pair] = dropped.get(pair, 0) + 1
                        continue
                    if name not in running_emitted:
                        running_emitted.add(name)
                        yield stream_status_message(by_name[name].stream, AirbyteStreamStatus.RUNNING)
                    record = _record_from_value(value)
                    records += 1
                    stream_state = sync_state.streams.setdefault(name, {})
                    stream_state["last_ts"] = max(int(stream_state.get("last_ts") or 0), record["_ts"])
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(stream=name, data=record, emitted_at=int(time.time() * 1000)),
                    )

                sync_state.cursor = page["pagination"]["nextCursor"]
                status = page.get("status", {})
                status_type = status.get("type")
                up_to_date = status_type in UP_TO_DATE_STATUSES
                if up_to_date:
                    for name in configured:
                        sync_state.streams.setdefault(name, {})["snapshot_complete"] = True
                        if "snapshotTs" in status:
                            sync_state.streams[name]["snapshot_ts"] = status["snapshotTs"]

                if pages % checkpoint_pages == 0:
                    yield from sync_state.to_messages(configured.keys())

                empty_page = not page.get("values") and not page.get("truncates")
                # `hasMore` stays true on an up to date sync, so completion is "up to date and nothing new on this page".
                if (up_to_date and empty_page) or not page["pagination"].get("hasMore", True):
                    break
                if max_pages and pages >= max_pages:
                    logger.info(
                        "Reached max_pages_per_sync=%d; checkpointing and stopping. The next sync resumes from the saved cursor.", max_pages
                    )
                    break
        except Exception:
            yield from sync_state.to_messages(configured.keys())
            for name in configured:
                yield stream_status_message(configured[name].stream, AirbyteStreamStatus.INCOMPLETE)
            raise

        yield from sync_state.to_messages(configured.keys())
        for name in configured:
            yield stream_status_message(configured[name].stream, AirbyteStreamStatus.COMPLETE)
        if dropped:
            logger.warning(
                "Dropped %d documents from tables not in the configured catalog: %s",
                sum(dropped.values()),
                ", ".join(f"{c or '<root>'}/{t}={n}" for (c, t), n in sorted(dropped.items())),
            )
        logger.info(
            "Convex data sync %s: %d pages, %d records, status=%s", sync_state.sync_id, pages, records, status_type if pages else "none"
        )

    @staticmethod
    def _configured_streams(catalog: ConfiguredAirbyteCatalog) -> Dict[str, ConfiguredAirbyteStream]:
        return {cs.stream.name: cs for cs in catalog.streams}

    @staticmethod
    def _pair_for(configured_stream: ConfiguredAirbyteStream) -> Tuple[str, str]:
        """Recover ``(component, table)`` from a stream. Prefers the schema's origin hint, else parses the name."""
        schema = configured_stream.stream.json_schema or {}
        if "x-convex-table" in schema:
            return schema.get("x-convex-component", ROOT_COMPONENT), schema["x-convex-table"]
        name = configured_stream.stream.name
        if COMPONENT_SEPARATOR not in name:
            return ROOT_COMPONENT, name
        *component_parts, table = name.split(COMPONENT_SEPARATOR)
        return "/".join(component_parts), table
