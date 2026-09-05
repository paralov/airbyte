# Convex Data Sync

This page contains the setup guide and reference information for the Convex Data Sync source connector.
This connector uses the Convex Deployment API and supports component tables.

Get started with Convex at the [Convex website](https://convex.dev).
See your data on the [Convex dashboard](https://dashboard.convex.dev/).

## Overview

The Convex source connector streams documents out of a Convex deployment with the
[Data Sync API](https://docs.convex.dev/deployment-api/data-sync). It supports Full Refresh,
Incremental Append, and Incremental Dedupe with deletes, and it syncs tables that live inside
installed [Convex components](https://docs.convex.dev/components) as well as tables in the app itself.

### Output schema

Every Convex table becomes one Airbyte stream named after the table. Tables inside a
[component](https://docs.convex.dev/components) carry the component path as the stream's namespace, for
example namespace `betterAuth` for `user`, or `resend/emailWorkpool` for `payload`. Tables in the root of
the app have no namespace and land in the destination's default namespace.

Where component tables end up is controlled by the connection's namespace setting:

- **Destination default** ignores namespaces. Only use this if no table name repeats across components.
- **Mirror source structure** puts each component in its own database or schema, named after the path.
- **Custom format** such as `convex_${SOURCE_NAMESPACE}` keeps everything side by side, e.g. `convex_betterAuth`.

Joins across components work as usual with qualified names, e.g. `convex.agentRun` with
`convex_betterAuth.user`.

Every record includes the fields defined for the table plus these system fields:

1. `_id` uniquely identifies the document. It is not changed by `.patch` or `.replace` operations.
2. `_creationTime` records a timestamp in milliseconds when the document was initially created.
3. `_ts` records the database timestamp in nanoseconds when this revision was written. It is the cursor.
4. `_deleted` is `true` for tombstones. Deleted documents only carry `_id` and the system fields.
5. `_component` and `_table` record where the document came from.
6. `_ab_cdc_lsn`, `_ab_cdc_updated_at`, and `_ab_cdc_deleted_at` follow the CDC convention used by
   database sources so destinations dedupe and delete consistently.

Records arrive in Convex's lossless export JSON (the same encoding as snapshot exports): int64 values
are plain numbers, while bytes become `{"$bytes": "<base64>"}` and non-finite floats become
`{"$float": "<base64>"}`.

### Table schemas

Schemas are opt-in in Convex, so the connector never asks the deployment for one. You tell it which tables
to sync and what they look like, in Convex's own terms: the "Table Schemas" field takes a JSON object of the
form `{"<component path>": {"<table>": <Convex validator JSON> | null}}`, with `""` as the root component
path. A validator is the `.json` form of the table's `v.object({...})` from `convex/values`, exactly what
`npx convex dev` pushes to your deployment. The connector converts it to JSON Schema for Airbyte.

- Tables you leave out are not synced.
- Use `null` for a table that exists without a schema. It gets a permissive stream schema (system fields
  typed, everything else allowed).
- Int64 fields are typed as integers; bytes fields arrive as `{"$bytes": "<base64>"}` objects.
- Table and component names must be valid Convex identifiers (letters, digits and underscores, at most 64
  characters; table names cannot start with `_`). Anything else is rejected when the source is tested.

Build this object from the deployed app's `schema.ts` files, including schemas of components installed
through `convex.config.ts`. Use each table's `validator.json` and the component's installed path.
Update the configuration and refresh the source schema in Airbyte after schema changes.

### State and sync behaviour

One Convex data sync covers every selected table, so there is a single Convex cursor. The connector
stores that same cursor in every stream's state at each checkpoint and resumes from the oldest saved
checkpoint. This replays changes, including deletes, when only some streams acknowledged a checkpoint;
already acknowledged records may be emitted again. Recovery checkpoints retain the rewind cursor until
all selected streams acknowledge the same up-to-date cursor, so repeated partial failures remain resumable.

- Deselected streams do not hold back the cursor.

- Enabling a new stream makes Convex sync that table from scratch; nothing else is re-sent.
- A Full Refresh stream, or a stream whose data you cleared in Airbyte, is deselected for one page
  and reselected, which makes Convex re-send it in full. Other streams keep streaming changes.
- The cursor expires after 3 days without a sync. The connector fails with a reset instruction:
  a fresh snapshot cannot recover deletes from the expired history. Clear the connection's data
  and sync again to rebuild the destination.
- The deployment URL is saved with the cursor. If its host no longer matches the source configuration, or
  Convex rejects the cursor (for example after changing the deployment URL), the sync fails with a
  configuration error instead of piling a fresh snapshot onto the old rows. Clear the connection's data
  and sync again.
- A Full Refresh stream is only complete once the sync is up to date, and Airbyte clears Full Refresh
  state between jobs, so its snapshot has to finish within one run: "Max Pages Per Sync" is not applied
  while Full Refresh streams are selected. If a run fails midway, the retry attempt continues the
  snapshot. Tombstones are never emitted into Full Refresh streams, so a document deleted while its
  snapshot was still in progress can survive in the destination until the next run; use Incremental
  Dedupe for an exact mirror.
- If Convex truncates a table (for example after `npx convex import --replace`), the connector logs
  a warning and the table is re-sent in full. Rows deleted by the truncate are not tombstoned, so
  reset that stream in the destination if you need an exact mirror.
- A sync stops at the first page on which Convex reports the export is up to date; the Incremental
  streams then hold a consistent snapshot of every selected table. Set "Max Pages Per Sync" to bound
  very large initial syncs, and deployments written faster than the connector reads them (Convex may
  never report such a sync up to date); the next run resumes from the saved cursor.
- A stream that is still in the connection but no longer in the table schemas is not synced: the
  connector logs a warning and reports that stream incomplete on every run (the other streams sync
  normally) until you refresh the source schema and remove it.
- A table in the schema JSON that does not exist in the deployment (a typo, or a table not created yet)
  receives no data: the connector logs a warning and reports that stream incomplete on every run until
  the table exists. The other streams are unaffected.

### Features

| Feature                       | Supported? |
| :---------------------------- | :--------- |
| Full Refresh Sync             | Yes        |
| Incremental - Append Sync     | Yes        |
| Incremental - Dedupe Sync     | Yes        |
| Replicate Incremental Deletes | Yes        |
| Change Data Capture           | Yes        |
| Component tables              | Yes        |
| Namespaces                    | Yes        |

### Performance considerations

The initial sync walks every selected table once, then streams changes. Pages are small, so a large
deployment takes many requests; the connector checkpoints state every 25 pages by default ("State
Checkpoint Interval"). Each request waits up to "Request Timeout" seconds and is retried with backoff
on transient errors.

## Getting started

### Requirements

- Convex account and project on a Pro plan (streaming export requires Pro)
- A deploy key with the `deployment:data:view` permission

### Setup guide

On the [Convex dashboard](https://dashboard.convex.dev/), navigate to the project that you want to sync.

1. Navigate to the Settings tab.
2. Copy the "Deployment URL" from the settings page to the `deployment_url` field in Airbyte.
3. Click "Generate a deploy key" and grant it `deployment:data:view`.
4. Copy the generated deploy key into the `access_key` field in Airbyte.
5. Paste the generated table schemas into the "Table Schemas" field.

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request                                             | Subject                                                          |
| :------ | :--------- | :------------------------------------------------------- | :--------------------------------------------------------------- |
| 0.1.0 | 2026-09-04 | [1](https://github.com/paralov/airbyte/pull/1) | New connector on the Convex Deployment API data sync endpoint: one resumable cursor, component tables as namespaced streams, schemas supplied as Convex validator JSON |

</details>
