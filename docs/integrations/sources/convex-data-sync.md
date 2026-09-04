# Convex Data Sync

This page contains the setup guide and reference information for the Convex Data Sync source connector.
It is a separate connector from the original Convex source, built from the ground up on the Convex Deployment API.

Get started with Convex at the [Convex website](https://convex.dev).
See your data on the [Convex dashboard](https://dashboard.convex.dev/).

## Overview

The Convex source connector streams documents out of a Convex deployment with the
[Data Sync API](https://docs.convex.dev/deployment-api/data-sync). It supports Full Refresh,
Incremental Append, and Incremental Dedupe with deletes, and it syncs tables that live inside
installed [Convex components](https://docs.convex.dev/components) as well as tables in the app itself.

### Output schema

Every Convex table becomes one Airbyte stream:

- Tables in the root of the app keep their name, for example `messages`.
- Tables inside a component are named `<component path>__<table>`, with `/` in nested component
  paths replaced by `__`. For example `betterAuth__user` or `resend__emailWorkpool__payload`.

Every record includes the fields defined for the table plus these system fields:

1. `_id` uniquely identifies the document. It is not changed by `.patch` or `.replace` operations.
2. `_creationTime` records a timestamp in milliseconds when the document was initially created.
3. `_ts` records the database timestamp in nanoseconds when this revision was written. It is the cursor.
4. `_deleted` is `true` for tombstones. Deleted documents only carry `_id` and the system fields.
5. `_component` and `_table` record where the document came from.
6. `_ab_cdc_lsn`, `_ab_cdc_updated_at`, and `_ab_cdc_deleted_at` follow the CDC convention used by
   database sources so destinations dedupe and delete consistently.

Values that JSON cannot represent are encoded as described in the
[Convex JSON format](https://docs.convex.dev/database/types), for example an int64 becomes
`{"$integer": "<base64>"}`.

### Table schemas

Airbyte needs a JSON Schema for each stream. The connector can get it two ways, chosen with the
"Table Schemas" option:

- **Fetch from deployment** asks the deployment for the schema of every table, including tables
  inside installed components. For tables without a schema the deployment infers one from the
  documents' shape; the request fails if any table's documents cannot be described as a single
  object shape, in which case use an inline schema. Because every sync re-fetches the schemas to
  check that the selected tables still exist, such a table breaks every sync of the connection, not
  only discovery.
- **Inline JSON** takes a JSON object of the form `{"<component path>": {"<table>": <JSON Schema>}}`,
  with `""` as the root component path. Use this to pin schemas or to expose only some tables. You
  can generate the object from your `convex.config.ts` and each component's `schema.ts` with a small
  script that reads every table validator's `.json` representation and converts it to JSON Schema.

Two tables that map to the same stream name (for example a root table `audit__log` and a table `log`
inside a component `audit`) are rejected; rename one of them or leave it out of an inline schema.

### State and sync behaviour

One Convex data sync covers every selected table, so there is a single Convex cursor. The connector
stores that same cursor in every stream's state at each checkpoint and resumes from the most recent
one. Consequences:

- Enabling a new stream makes Convex sync that table from scratch; nothing else is re-sent.
- A Full Refresh stream, or a stream whose data you cleared in Airbyte, is deselected for one page
  and reselected, which makes Convex re-send it in full. Other streams keep streaming changes.
- The cursor expires after 3 days without a sync. The connector then restarts from scratch and
  re-sends everything. Incremental Dedupe destinations absorb the re-sent rows, but rows deleted
  while the cursor was expired are not tombstoned, so reset the streams in the destination if you
  need an exact mirror.
- A Full Refresh stream is only complete once the sync is up to date. If "Max Pages Per Sync" stops
  the run before that, the stream is reported incomplete (the destination keeps its previous data)
  and the next run continues its snapshot; tombstones are never emitted into Full Refresh streams.
- If Convex truncates a table (for example after `npx convex import --replace`), the connector logs
  a warning and the table is re-sent in full. Rows deleted by the truncate are not tombstoned, so
  reset that stream in the destination if you need an exact mirror.
- A sync stops at the first page on which Convex reports the export is up to date; that page is a
  consistent snapshot of every selected table. Set "Max Pages Per Sync" to bound very large initial
  syncs; the next run resumes from the saved cursor.
- A stream that is still in the connection but no longer in the deployment (or the inline schema) is
  skipped with a warning; refresh the source schema to remove it.

### Features

| Feature                       | Supported? |
| :---------------------------- | :--------- |
| Full Refresh Sync             | Yes        |
| Incremental - Append Sync     | Yes        |
| Incremental - Dedupe Sync     | Yes        |
| Replicate Incremental Deletes | Yes        |
| Change Data Capture           | Yes        |
| Component tables              | Yes        |
| Namespaces                    | No         |

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
Only "Production" deployments should be synced.

1. Navigate to the Settings tab.
2. Copy the "Deployment URL" from the settings page to the `deployment_url` field in Airbyte.
3. Click "Generate a deploy key" and grant it `deployment:data:view`.
4. Copy the generated deploy key into the `access_key` field in Airbyte.
5. Choose a schema source. "Fetch from deployment" covers root and component tables.

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request                                             | Subject                                                          |
| :------ | :--------- | :------------------------------------------------------- | :--------------------------------------------------------------- |
| 0.1.0 | 2026-09-04 | [1](https://github.com/paralov/airbyte/pull/1) | New connector on the Convex Deployment API data sync endpoint: one resumable cursor, component tables as streams, inline schema option |

</details>
