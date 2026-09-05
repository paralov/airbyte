#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

"""Convex source built on the Deployment API data sync endpoint.

One Convex data sync (``POST /api/v1/data/sync``) streams every selected table
in the deployment, including tables that live inside installed components, and
returns a single opaque cursor per page. That maps onto Airbyte as:

* one Airbyte stream per ``(component, table)`` pair: the stream is named after
  the table and its namespace is the component path (root tables use the
  destination's default namespace);
* per-stream Airbyte state that each carry the same Convex cursor (the CDK
  entrypoint's record counting requires per-stream state), plus a little
  per-stream bookkeeping;
* the configured catalog driving the Convex ``selection`` so the deployment only
  ships tables Airbyte asked for;
* table schemas supplied by the user as Convex validator JSON (schemas are
  opt-in in Convex, so the connector never asks the deployment for one).

See https://docs.convex.dev/deployment-api/data-sync.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import math
import re
import sqlite3
import struct
import time
from contextlib import ExitStack, closing
from datetime import datetime, timezone
from tempfile import TemporaryDirectory
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple
from urllib.parse import urlsplit

import requests

from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    AirbyteStreamStatus,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    Status,
    StreamDescriptor,
    SyncMode,
    Type,
)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.utils.stream_status_utils import as_airbyte_message as stream_status_message
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType


LOGGER = logging.getLogger("airbyte")

CONVEX_CLIENT_VERSION = "0.1.0"
# Convex recognises the versioned ``airbyte-export-`` client prefix and gives
# these syncs an ``airbyte-`` sync ID.
CONVEX_CLIENT_HEADER = f"airbyte-export-{CONVEX_CLIENT_VERSION}"

ROOT_COMPONENT = ""
# Reserved key at every level of a Convex ``selection``: the default for names not listed explicitly.
SELECTION_OTHER = "_other"

# Convex table and component names are identifiers of at most 64 characters; user tables never start with ``_``.
CONVEX_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

# The documented ``status.type`` values are ``snapshotting``, ``stale`` and ``upToDate``.
# ``pagination.hasMore`` is documented as always true, so ``upToDate`` is the only completion signal.
UP_TO_DATE_STATUS = "upToDate"

# Neither an expired cursor nor a cursor from another deployment can safely resume
# incremental deletes. A fresh snapshot would leave deleted rows in the destination.
UNUSABLE_CURSOR_ERROR_CODES = {"DataSyncCursorExpired", "InvalidDataSyncCursor"}

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
# A connection check should answer quickly: one retry is enough to ride out a blip.
CHECK_MAX_ATTEMPTS = 2
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
    # Same nanosecond timestamp as ``_ts``; ``integer`` so destinations keep it exact (it exceeds 2**53).
    "_ab_cdc_lsn": {"type": "integer"},
    "_ab_cdc_updated_at": {"type": "string"},
    "_ab_cdc_deleted_at": {"anyOf": [{"type": "string"}, {"type": "null"}]},
}


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------


StreamKey = Tuple[str, str]  # (component path, table); the root component is ""


def namespace_for(component: str) -> Optional[str]:
    """Airbyte namespace of a table: the component path, or the destination default for the root component."""
    return component or None


def stream_key(name: str, namespace: Optional[str]) -> StreamKey:
    return (namespace or ROOT_COMPONENT, name)


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


def _retry_delay(resp: Optional[requests.Response], attempt: int) -> float:
    """Seconds to wait before retrying: the server's Retry-After when it sent a numeric one, else exponential backoff."""
    retry_after = resp.headers.get("Retry-After") if resp is not None else None
    if retry_after:
        try:
            seconds = float(retry_after)
        except ValueError:
            seconds = math.nan  # HTTP-date form; fall back to exponential backoff.
        # ``float`` also accepts "nan" and "inf", which survive the clamp and make ``time.sleep`` raise.
        if math.isfinite(seconds):
            return min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS)
    return min(2**attempt, MAX_BACKOFF_SECONDS)


class ConvexClient:
    """Thin client over the deployment endpoints this connector needs."""

    def __init__(
        self,
        deployment_url: str,
        access_key: str,
        timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        session: Optional[requests.Session] = None,
        max_attempts: int = MAX_ATTEMPTS,
    ):
        self.deployment_url = deployment_url.rstrip("/")
        self.timeout = timeout
        self.max_attempts = max_attempts
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
                if attempt >= self.max_attempts:
                    raise ConvexApiError(context, 0, "RequestException", str(e)) from e
                delay = _retry_delay(None, attempt)
                LOGGER.info("%s: %s; retrying in %.0fs (attempt %d/%d)", context, e, delay, attempt, self.max_attempts)
                time.sleep(delay)
                continue
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as e:
                    raise ConvexApiError(context, 200, "InvalidJSON", resp.text[:200]) from e
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < self.max_attempts:
                delay = _retry_delay(resp, attempt)
                LOGGER.info("%s: HTTP %d; retrying in %.0fs (attempt %d/%d)", context, resp.status_code, delay, attempt, self.max_attempts)
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


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


TableSchemas = Dict[Tuple[str, str], Optional[Dict[str, Any]]]  # (component, table) -> Convex validator JSON, or None

# The data sync endpoint always uses Convex's lossless export JSON (the snapshot export format): int64 values are
# plain JSON numbers, while bytes and non-finite floats are wrapped as ``{"$bytes": ...}`` / ``{"$float": ...}``.
INT64_SCHEMA: Dict[str, Any] = {"type": "integer"}
BYTES_SCHEMA: Dict[str, Any] = {"type": "object", "properties": {"$bytes": {"type": "string"}}, "required": ["$bytes"]}
FLOAT_SCHEMA: Dict[str, Any] = {
    "anyOf": [{"type": "number"}, {"type": "object", "properties": {"$float": {"type": "string"}}, "required": ["$float"]}]
}


def _invalid_validator(detail: str) -> AirbyteTracedException:
    return AirbyteTracedException(message=f"Invalid Convex validator in the schema JSON: {detail}.", failure_type=FailureType.config_error)


def _literal_bytes(encoded: str, tag: str) -> bytes:
    """Numeric validator literals use base64 little-endian eight-byte values."""
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as e:
        raise _invalid_validator(f"literal {{{tag!r}: {encoded!r}}} is not base64") from e
    if len(raw) != 8:
        raise _invalid_validator(f"literal {{{tag!r}: {encoded!r}}} must contain eight bytes")
    return raw


def validator_to_json_schema(validator: Mapping[str, Any]) -> Dict[str, Any]:
    """Convert Convex validator JSON (the ``.json`` of ``v.object({...})`` etc.) to a draft-07 JSON Schema.

    Convex schemas are opt-in and validators are the only schema primitive Convex has, so the connector takes
    them verbatim rather than inventing its own format. Nested objects never set ``additionalProperties: false``:
    Airbyte requires every ``additionalProperties`` in a stream schema to be true, and it keeps records valid
    when a nested field is added in Convex before the schema here is refreshed.

    The JSON is user-supplied, so every nested shape is checked and a malformed one is a config error rather
    than a stack trace out of check/discover/read.
    """
    if not isinstance(validator, Mapping):
        raise _invalid_validator(f"expected a validator object, got {validator!r}")
    kind = validator.get("type")
    if kind == "null":
        return {"type": "null"}
    if kind == "number":
        return dict(FLOAT_SCHEMA)
    if kind == "boolean":
        return {"type": "boolean"}
    if kind == "string":
        return {"type": "string"}
    if kind == "any":
        return {}
    if kind in ("bigint", "commitTs"):
        return dict(INT64_SCHEMA)
    if kind == "bytes":
        return dict(BYTES_SCHEMA)
    if kind == "literal":
        value = validator.get("value")
        if isinstance(value, Mapping) and isinstance(value.get("$integer"), str):
            # The data sync export ships int64 values as plain JSON numbers, so the enum holds the decoded integer.
            return {"type": "integer", "enum": [int.from_bytes(_literal_bytes(value["$integer"], "$integer"), "little", signed=True)]}
        if isinstance(value, Mapping) and isinstance(value.get("$float"), str):
            (number,) = struct.unpack("<d", _literal_bytes(value["$float"], "$float"))
            if math.isfinite(number):
                # Validator JSON wraps -0, but export JSON sends it as -0.0.
                return {"type": "number", "enum": [number]}
            # NaNs can have different payload bits; do not require a particular encoding.
            return {"type": "object", "properties": {"$float": {"type": "string"}}, "required": ["$float"]}
        # Untagged numeric literals are Convex float64 values, including JSON integers.
        json_type = {bool: "boolean", int: "number", float: "number", str: "string"}.get(type(value))
        if json_type is None:
            raise _invalid_validator(f"unsupported literal value {value!r}")
        return {"type": json_type, "enum": [value]}
    if kind == "id":
        return {"type": "string", "$description": f"Id({validator.get('tableName', '?')})"}
    if kind == "array":
        return {"type": "array", "items": validator_to_json_schema(validator.get("value") or {"type": "any"})}
    if kind == "record":
        return {"type": "object", "additionalProperties": True}
    if kind == "object":
        fields = validator.get("value") or {}
        if not isinstance(fields, Mapping):
            raise _invalid_validator(f"object fields must be an object, got {fields!r}")
        properties: Dict[str, Any] = {}
        required: List[str] = []
        for name, field in fields.items():
            field_type = field.get("fieldType") if isinstance(field, Mapping) and "fieldType" in field else field
            properties[name] = validator_to_json_schema(field_type or {"type": "any"})
            if not (isinstance(field, Mapping) and field.get("optional")):
                required.append(name)
        return {"type": "object", "properties": properties, "required": required}
    if kind == "union":
        members = validator.get("value")
        if not isinstance(members, list) or not members:
            # ``{"anyOf": []}`` is not valid draft-07, so an empty union cannot become a stream schema.
            raise _invalid_validator(f"a union needs a non-empty list of members, got {members!r}")
        return {"anyOf": [validator_to_json_schema(member) for member in members]}
    raise _invalid_validator(f"unknown validator type {kind!r}")


def _check_table_identifiers(component: str, table: str) -> None:
    """Reject names Convex would refuse in a ``selection`` (HTTP 400 InvalidDataSyncSelection) before the first sync."""
    for segment in component.split("/") if component else []:
        if not CONVEX_IDENTIFIER.fullmatch(segment):
            raise AirbyteTracedException(
                message=f'Schema entry {component!r} is not a valid Convex component path (use "" for the root component).',
                failure_type=FailureType.config_error,
            )
        if segment == SELECTION_OTHER:
            # The identifier grammar allows it, but a selection can never address a component by that name.
            raise AirbyteTracedException(
                message=f"Schema entry {component!r}: {SELECTION_OTHER!r} is reserved in Convex selections and cannot name a component.",
                failure_type=FailureType.config_error,
            )
    if not CONVEX_IDENTIFIER.fullmatch(table) or table.startswith("_"):
        raise AirbyteTracedException(
            message=f"{component or '<root>'}/{table!r} is not a valid Convex table name.",
            failure_type=FailureType.config_error,
        )


def parse_schema_json(schema_json: str) -> TableSchemas:
    """Parses ``{"<component path>": {"<table>": <Convex validator JSON> | null}}``, with ``""`` for the root component.

    ``null`` marks a table that exists but has no schema; it gets a permissive stream schema.
    """
    try:
        parsed = json.loads(schema_json)
    except ValueError as e:
        raise AirbyteTracedException(
            message="The schema JSON is not valid JSON.",
            internal_message=str(e),
            failure_type=FailureType.config_error,
        ) from e
    if not isinstance(parsed, dict):
        raise AirbyteTracedException(message="The schema JSON must be an object.", failure_type=FailureType.config_error)
    out: TableSchemas = {}
    for component, tables in parsed.items():
        if not isinstance(tables, dict):
            raise AirbyteTracedException(
                message=f"Schema entry {component!r} must map table names to Convex validators (or null).",
                failure_type=FailureType.config_error,
            )
        for table, validator in tables.items():
            if validator is not None and not isinstance(validator, dict):
                # A string here is the signature of the flat form ``{"<table>": {"type": ..., ...}}``.
                hint = (
                    f' If {component!r} is a table, wrap it under the root component path: {{"": {{"{component}": <validator>}}}}.'
                    if isinstance(validator, str)
                    else ""
                )
                raise AirbyteTracedException(
                    message=f"Schema for table {component!r}/{table!r} must be a Convex validator object or null.{hint}",
                    failure_type=FailureType.config_error,
                )
            _check_table_identifiers(component, table)
            out[(component, table)] = validator
    return out


def airbyte_schema_for(validator: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Stream schema for a table: its validator as JSON Schema (or nothing, for schema-less tables) plus system + CDC fields."""
    schema: Dict[str, Any] = {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object"}
    table_schema = validator_to_json_schema(validator) if validator is not None else {}
    # Airbyte destinations read top-level properties; expose every document-union
    # branch there, including branches nested inside other unions.
    properties: Dict[str, Any] = {}
    branches = [table_schema]
    while branches:
        branch = branches.pop()
        branches.extend(branch.get("anyOf", []))
        for name, field_schema in branch.get("properties", {}).items():
            if name in properties and properties[name] != field_schema:
                properties[name] = {"anyOf": [properties[name], field_schema]}
            else:
                properties[name] = field_schema
    properties.update(SYSTEM_FIELD_SCHEMAS)
    schema["properties"] = properties
    schema["additionalProperties"] = True
    return schema


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------


class ConvexTableStream(Stream):
    primary_key = "_id"
    cursor_field = "_ts"

    def __init__(self, component: str, table: str, validator: Optional[Mapping[str, Any]]):
        self.component = component
        self.table = table
        self._json_schema = airbyte_schema_for(validator)

    @property
    def name(self) -> str:
        return self.table

    @property
    def namespace(self) -> Optional[str]:
        return namespace_for(self.component)

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
    checkpoint along with a ``checkpointed_at`` stamp. The stamp is a monotonic
    sequence number continued from the highest incoming stamp. Resume from the
    oldest saved checkpoint so changes not acknowledged by every stream, including
    deletes, are replayed. During recovery, checkpoints also retain that rewind
    point until all streams agree on an up-to-date cursor: a later checkpoint can still be
    behind an earlier attempt's cursor. Already acknowledged records may repeat.

    The deployment URL is stored alongside the cursor so a cursor is never sent to a
    different deployment than the one that minted it.
    """

    SHARED_KEYS = ("cursor", "sync_id", "deployment_url", "checkpointed_at", "replay_from", "up_to_date")

    def __init__(self) -> None:
        self.cursor: Optional[str] = None
        self.sync_id: Optional[str] = None
        self.deployment_url: Optional[str] = None
        self.checkpoint_seq = 0
        self.replay_from: Optional[Dict[str, Any]] = None
        self.up_to_date = False
        self.streams: Dict[StreamKey, Dict[str, Any]] = {}

    @classmethod
    def _stream_keys(cls, blob: Mapping[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in blob.items() if k not in cls.SHARED_KEYS}

    @classmethod
    def from_messages(cls, logger: logging.Logger, state: Optional[List[AirbyteStateMessage]], selected: Set[StreamKey]) -> "SyncState":
        out = cls()
        if not state:
            return out
        blobs: Dict[StreamKey, Dict[str, Any]] = {}
        for message in state:
            if message.type == AirbyteStateType.STREAM and message.stream is not None:
                blob = _blob_to_dict(message.stream.stream_state)
                if "cursor" in blob:
                    descriptor = message.stream.stream_descriptor
                    key = stream_key(descriptor.name, descriptor.namespace)
                    if key in selected:
                        blobs[key] = blob
        if not blobs:
            logger.warning("No resumable Convex cursor in the incoming state; starting a fresh data sync.")
            return out
        for blob in blobs.values():
            replay_from = blob.get("replay_from")
            if replay_from is not None and not isinstance(replay_from, Mapping):
                raise AirbyteTracedException(
                    message="The saved state's replay_from is not an object. Clear the connection's data and sync again.",
                    failure_type=FailureType.config_error,
                )
        try:
            out.checkpoint_seq = max(int(blob.get("checkpointed_at") or 0) for blob in blobs.values())
            diverged = len({blob.get("cursor") for blob in blobs.values()}) > 1
            candidates = [blob.get("replay_from") or blob for blob in blobs.values()] if diverged else list(blobs.values())
            oldest = min(candidates, key=lambda c: int(c.get("checkpointed_at") or 0))
            if diverged:
                out.replay_from = {key: oldest.get(key) for key in cls.SHARED_KEYS if key != "replay_from"}
            elif not all(blob.get("up_to_date") for blob in blobs.values()):
                # Agreement on a capped recovery page does not establish that it
                # passed older checkpoints belonging to deselected streams.
                rewind_points = [blob["replay_from"] for blob in blobs.values() if blob.get("replay_from")]
                if rewind_points:
                    out.replay_from = min(rewind_points, key=lambda c: int(c.get("checkpointed_at") or 0))
        except (TypeError, ValueError) as e:
            # Only hand-edited state gets here; the connector always writes integer stamps.
            raise AirbyteTracedException(
                message="The saved state's checkpointed_at is not a number. Clear the connection's data and sync again.",
                internal_message=str(e),
                failure_type=FailureType.config_error,
            ) from e
        out.cursor = oldest.get("cursor")
        out.sync_id = oldest.get("sync_id")
        out.deployment_url = oldest.get("deployment_url")
        out.up_to_date = oldest.get("up_to_date") is True
        out.streams = {key: cls._stream_keys(blob) for key, blob in blobs.items()}
        return out

    def stream(self, key: StreamKey) -> Dict[str, Any]:
        return self.streams.setdefault(key, {})

    def to_messages(self, keys: Iterable[StreamKey]) -> Iterator[AirbyteMessage]:
        self.checkpoint_seq += 1
        for component, table in keys:
            blob = AirbyteStateBlob(
                cursor=self.cursor,
                sync_id=self.sync_id,
                deployment_url=self.deployment_url,
                checkpointed_at=self.checkpoint_seq,
                up_to_date=self.up_to_date,
                **({"replay_from": self.replay_from} if self.replay_from else {}),
                **self.streams.get((component, table), {}),
            )
            yield AirbyteMessage(
                type=Type.STATE,
                state=AirbyteStateMessage(
                    type=AirbyteStateType.STREAM,
                    stream=AirbyteStreamState(
                        stream_descriptor=StreamDescriptor(name=table, namespace=namespace_for(component)), stream_state=blob
                    ),
                ),
            )


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


def _describe(key: StreamKey) -> str:
    return f"{key[0] or '<root>'}/{key[1]}"


def build_selection(pairs: Iterable[StreamKey]) -> Dict[str, Any]:
    """Convex ``selection`` body that includes exactly the given ``(component, table)`` pairs."""
    selection: Dict[str, Any] = {SELECTION_OTHER: "excluded"}
    for component, table in pairs:
        component_selection = selection.setdefault(component, {SELECTION_OTHER: "excluded"})
        component_selection[table] = {SELECTION_OTHER: "included"}
    return selection


def _deployment_host(url: str) -> str:
    """What identifies a deployment in its URL: scheme, case and trailing slashes do not."""
    return urlsplit(url).netloc.lower()


def _record_from_value(value: Mapping[str, Any]) -> Dict[str, Any]:
    # The document is annotated in place: the page is discarded after its values are emitted, and the CDK
    # copies the record before transforming it.
    record: Dict[str, Any] = value["value"]
    ts_ns = int(value["ts"])
    deleted = bool(value.get("deleted", False))
    # Naive UTC ISO string, matching the format used by the original Convex source connector.
    seconds, nanoseconds = divmod(ts_ns, 1_000_000_000)
    ts_iso = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(microsecond=nanoseconds // 1000, tzinfo=None).isoformat()
    record["_ts"] = ts_ns
    record["_deleted"] = deleted
    record["_component"] = value["component"]
    record["_table"] = value["table"]
    # Same CDC columns as Debezium-based sources so destinations dedupe and delete consistently.
    record["_ab_cdc_lsn"] = ts_ns
    record["_ab_cdc_updated_at"] = ts_iso
    record["_ab_cdc_deleted_at"] = ts_iso if deleted else None
    return record


def _next_cursor(page: Mapping[str, Any]) -> str:
    """The page's ``pagination.nextCursor``; the API types it as nullable, and a missing cursor must never silently restart the sync."""
    pagination = page.get("pagination")
    next_cursor = pagination.get("nextCursor") if isinstance(pagination, dict) else None
    if not isinstance(next_cursor, str) or not next_cursor:
        raise AirbyteTracedException(
            message="Convex returned a data sync page without a resumable nextCursor.",
            internal_message=json.dumps(pagination),
            failure_type=FailureType.system_error,
        )
    return next_cursor


class FullRefreshSnapshot:
    """Keep the latest live revision on disk until Convex reaches a consistent snapshot."""

    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        connection.execute(
            "CREATE TABLE documents (component TEXT, table_name TEXT, id TEXT, record TEXT, "
            "PRIMARY KEY (component, table_name, id)) WITHOUT ROWID"
        )

    def truncate(self, key: StreamKey) -> None:
        self.connection.execute("DELETE FROM documents WHERE component = ? AND table_name = ?", key)

    def apply(self, key: StreamKey, value: Mapping[str, Any]) -> None:
        if value.get("deleted"):
            self.connection.execute(
                "DELETE FROM documents WHERE component = ? AND table_name = ? AND id = ?", (*key, value["value"]["_id"])
            )
        else:
            record = _record_from_value(value)
            self.connection.execute("INSERT OR REPLACE INTO documents VALUES (?, ?, ?, ?)", (*key, record["_id"], json.dumps(record)))

    def records(self, key: StreamKey) -> Iterator[Dict[str, Any]]:
        for (record,) in self.connection.execute("SELECT record FROM documents WHERE component = ? AND table_name = ?", key):
            yield json.loads(record)


class SourceConvexDataSync(AbstractSource):
    def _client(self, config: Mapping[str, Any], max_attempts: int = MAX_ATTEMPTS) -> ConvexClient:
        return ConvexClient(
            config["deployment_url"],
            config["access_key"],
            timeout=int(config.get("request_timeout_seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS)),
            max_attempts=max_attempts,
        )

    # -- schema -----------------------------------------------------------

    @staticmethod
    def _table_schemas(config: Mapping[str, Any]) -> TableSchemas:
        return parse_schema_json(config.get("schema_json") or "")

    # -- check / discover -------------------------------------------------

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        # AbstractSource.check wraps the failure reason in repr(), which shows the user a quoted, escaped string.
        ok, error = self.check_connection(logger, config)
        if ok:
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        return AirbyteConnectionStatus(status=Status.FAILED, message=str(error))

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        client = self._client(config, max_attempts=CHECK_MAX_ATTEMPTS)
        try:
            if not self._streams(config):
                return False, "The schema JSON lists no tables."
            client.list_active_syncs()
        except AirbyteTracedException as e:
            return False, e.message
        except ConvexApiError as e:
            return False, str(e)
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return list(self._streams(config))

    def _streams(self, config: Mapping[str, Any]) -> List[ConvexTableStream]:
        schemas = self._table_schemas(config)
        return [ConvexTableStream(component, table, schemas[(component, table)]) for component, table in sorted(schemas)]

    # -- read -------------------------------------------------------------

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[List[AirbyteStateMessage]] = None,
    ) -> Iterator[AirbyteMessage]:
        client = self._client(config)
        if not catalog.streams:
            logger.info("No streams selected; nothing to sync.")
            return
        sync_state = SyncState.from_messages(logger, state, {stream_key(cs.stream.name, cs.stream.namespace) for cs in catalog.streams})
        if (
            sync_state.cursor is not None
            and sync_state.deployment_url is not None
            and _deployment_host(sync_state.deployment_url) != _deployment_host(client.deployment_url)
        ):
            raise AirbyteTracedException(
                message=(
                    f"The saved sync cursor belongs to the deployment {sync_state.deployment_url}, but the source is now configured "
                    f"for {client.deployment_url}. Clear the connection's data so the new deployment is synced from scratch."
                ),
                failure_type=FailureType.config_error,
            )
        sync_state.deployment_url = client.deployment_url
        max_pages = int(config.get("max_pages_per_sync") or 0)
        checkpoint_pages = int(config.get("state_checkpoint_pages") or STATE_CHECKPOINT_PAGES)

        # Every configured stream is a (component, table) pair: namespace + name. Keep the ones the schema JSON knows.
        known = set(self._table_schemas(config))
        selected: Dict[StreamKey, ConfiguredAirbyteStream] = {}
        for cs in catalog.streams:
            key = stream_key(cs.stream.name, cs.stream.namespace)
            if key not in known:
                logger.warning(
                    "The stream %s in your connection configuration was not found in the source; it is reported incomplete until "
                    "you refresh the source schema in your connection or add the table back to the Table Schemas field.",
                    _describe(key),
                )
                yield stream_status_message(cs.stream, AirbyteStreamStatus.INCOMPLETE)
                continue
            selected[key] = cs
        if not selected:
            return

        full_selection = build_selection(selected)
        resuming = sync_state.cursor is not None

        for cs in selected.values():
            yield stream_status_message(cs.stream, AirbyteStreamStatus.STARTED)

        # Full refresh is rebuilt on every attempt because its buffered rows are
        # local to this process. Clearing a stream's state also requests a snapshot.
        # Deselecting and reselecting makes Convex start a fresh table traversal.
        full_refresh = {name for name, cs in selected.items() if cs.sync_mode == SyncMode.full_refresh}
        priming_selection: Optional[Dict[str, Any]] = None
        resnapshot: Set[StreamKey] = set()
        if resuming:
            resnapshot = {name for name in selected if name not in sync_state.streams or name in full_refresh}
            if resnapshot:
                priming_selection = build_selection(key for key in selected if key not in resnapshot)
                for name in resnapshot:
                    sync_state.stream(name)["snapshot_complete"] = False
                    sync_state.stream(name)["awaiting_truncate"] = True
                    sync_state.stream(name)["has_records"] = False
                logger.info("Asking Convex to re-sync %s from scratch.", [_describe(key) for key in sorted(resnapshot)])
        for name in selected:
            sync_state.stream(name).setdefault("snapshot_complete", False)
            # Older states do not tell us whether destination rows exist.
            sync_state.stream(name).setdefault("has_records", resuming and name not in resnapshot)

        # A table's first truncate proves it exists, even if its snapshot spans runs.
        for name in selected:
            sync_state.stream(name).setdefault("awaiting_truncate", not resuming)
        missing: Set[StreamKey] = set()

        # A full refresh snapshot is only complete once the sync is up to date, and the platform clears full refresh
        # state between jobs, so stopping early would make the next run start the snapshot over: the page cap is not
        # applied while full refresh streams are selected.
        if max_pages and full_refresh:
            logger.info(
                "max_pages_per_sync=%d is not applied while the full refresh streams %s are selected: their snapshot has to "
                "finish within one run.",
                max_pages,
                [_describe(key) for key in sorted(full_refresh)],
            )
            max_pages = 0

        full_refresh_emitted = False

        def checkpoint() -> Iterator[AirbyteMessage]:
            # A local buffer is not durable Airbyte output. A retry must rebuild it.
            return sync_state.to_messages(key for key in selected if key not in full_refresh or full_refresh_emitted)

        def finish(status: AirbyteStreamStatus, checkpoint_state: bool = True) -> Iterator[AirbyteMessage]:
            if checkpoint_state:
                yield from checkpoint()
            for key, cs in selected.items():
                yield stream_status_message(cs.stream, AirbyteStreamStatus.INCOMPLETE if key in missing else status)

        running_emitted: Set[StreamKey] = set()
        dropped: Dict[StreamKey, int] = {}
        pages = 0
        records = 0
        status_type: Optional[str] = None

        def emit_record(name: StreamKey, record: Dict[str, Any]) -> Iterator[AirbyteMessage]:
            nonlocal records
            if name not in running_emitted:
                running_emitted.add(name)
                yield stream_status_message(selected[name].stream, AirbyteStreamStatus.RUNNING)
            records += 1
            sync_state.stream(name)["has_records"] = True
            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream=name[1], namespace=namespace_for(name[0]), data=record, emitted_at=int(time.time() * 1000)
                ),
            )

        try:
            with ExitStack() as resources:
                snapshot = None
                if full_refresh:
                    directory = resources.enter_context(TemporaryDirectory(prefix="convex-full-refresh-"))
                    connection = resources.enter_context(closing(sqlite3.connect(f"{directory}/snapshot.sqlite")))
                    snapshot = FullRefreshSnapshot(connection)
                while True:
                    priming = priming_selection is not None
                    selection = priming_selection if priming else full_selection
                    try:
                        page = client.data_sync(sync_state.cursor, selection)
                    except ConvexApiError as e:
                        if e.code in UNUSABLE_CURSOR_ERROR_CODES and sync_state.cursor is not None:
                            reason = (
                                "The saved Convex cursor expired. A fresh snapshot cannot recover deletes from the expired history."
                                if e.code == "DataSyncCursorExpired"
                                else "Convex rejected the saved cursor. The deployment URL may have changed since the last sync."
                            )
                            raise AirbyteTracedException(
                                message=f"{reason} Clear the connection's data and sync again.",
                                internal_message=str(e),
                                failure_type=FailureType.config_error,
                            ) from e
                        raise e.as_traced() from e
                    next_cursor = _next_cursor(page)
                    priming_selection = None

                    sync_state.sync_id = page.get("syncId", sync_state.sync_id)

                    truncates = {
                        (truncate.get("component", ROOT_COMPONENT), truncate["table"]) for truncate in page.get("truncates", [])
                    } & selected.keys()
                    for name in truncates - full_refresh:
                        if sync_state.stream(name).get("has_records"):
                            raise AirbyteTracedException(
                                message=(
                                    f"Convex restarted table {_describe(name)} after records were emitted. A new snapshot cannot "
                                    "remove previously replicated rows. Clear this stream's data in Airbyte and sync again."
                                ),
                                failure_type=FailureType.config_error,
                            )
                    for name in truncates:
                        if snapshot is not None and name in full_refresh:
                            snapshot.truncate(name)
                        sync_state.stream(name)["awaiting_truncate"] = False
                        sync_state.stream(name)["snapshot_complete"] = False

                    for value in page.get("values", []):
                        name = (value.get("component", ROOT_COMPONENT), value["table"])
                        if name not in selected:
                            dropped[name] = dropped.get(name, 0) + 1
                            continue
                        if snapshot is not None and name in full_refresh:
                            snapshot.apply(name, value)
                        else:
                            yield from emit_record(name, _record_from_value(value))

                    sync_state.cursor = next_cursor
                    pages += 1
                    status_type = page.get("status", {}).get("type")
                    up_to_date = status_type == UP_TO_DATE_STATUS
                    sync_state.up_to_date = up_to_date and not priming
                    # The priming page describes a sync that still excludes the re-snapshot streams, so it never
                    # counts as complete for them.
                    if up_to_date and not priming:
                        missing = {name for name in selected if sync_state.stream(name).get("awaiting_truncate")}
                        if missing:
                            logger.warning(
                                "Convex never started syncing %s: these tables do not exist in the deployment (check the Table Schemas "
                                "field for typos). They are reported incomplete; the other streams are complete.",
                                [_describe(key) for key in sorted(missing)],
                            )
                        for name in selected:
                            if name not in missing:
                                sync_state.stream(name)["snapshot_complete"] = True

                    if pages % checkpoint_pages == 0:
                        yield from checkpoint()

                    if priming:
                        continue  # Always fetch at least one page with the full selection so the deselected tables come back.
                    # An upToDate page is a consistent snapshot through its snapshotTs. `hasMore` is always true on a
                    # live sync and a busy deployment can keep upToDate pages non-empty forever, so stop here.
                    if up_to_date:
                        break
                    if max_pages and pages >= max_pages:
                        logger.info(
                            "Reached max_pages_per_sync=%d; checkpointing and stopping. The next sync resumes from the saved cursor.",
                            max_pages,
                        )
                        break

                if snapshot is not None:
                    for name in sorted(full_refresh - missing):
                        for record in snapshot.records(name):
                            yield from emit_record(name, record)
                full_refresh_emitted = True
        except Exception:
            # With no completed page, preserve the original state so a failed
            # priming page cannot cancel a cleared stream's pending snapshot.
            yield from finish(AirbyteStreamStatus.INCOMPLETE, checkpoint_state=pages > 0)
            raise

        yield from finish(AirbyteStreamStatus.COMPLETE)
        if dropped:
            logger.warning(
                "Dropped %d documents from tables not in the configured catalog: %s",
                sum(dropped.values()),
                ", ".join(f"{_describe(key)}={n}" for key, n in sorted(dropped.items())),
            )
        logger.info("Convex data sync %s: %d pages, %d records, status=%s", sync_state.sync_id, pages, records, status_type or "none")
