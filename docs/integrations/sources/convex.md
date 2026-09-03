# Convex

This page contains the setup guide and reference information for the Convex source connector.

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

- **Fetch from deployment** asks the deployment for its schemas. This only describes tables in the
  root component; tables inside components are not discovered.
- **Inline JSON** takes a JSON object of the form `{"<component path>": {"<table>": <JSON Schema>}}`,
  with `""` as the root component path. Use this to sync component tables. You can generate the
  object from your `convex.config.ts` and each component's `schema.ts` with a small script that reads
  every table validator's `.json` representation and converts it to JSON Schema.

### State and sync behaviour

One Convex data sync covers every selected table, so there is a single Convex cursor. The connector
stores that same cursor in every stream's state at each checkpoint and resumes from the most recent
one. Consequences:

- Enabling a new stream makes Convex sync that table from scratch; nothing else is re-sent.
- A Full Refresh stream is deselected for one page and reselected, which makes Convex re-send it
  in full. Other streams keep streaming changes.
- The cursor expires after 3 days without a sync. The connector then restarts from scratch and
  re-sends everything; Incremental Dedupe destinations absorb this.
- If Convex truncates a table (for example after `npx convex import --replace`), the connector logs
  a warning and the table is re-sent in full. Rows deleted by the truncate are not tombstoned, so
  reset that stream in the destination if you need an exact mirror.
- A sync stops when Convex reports the export is up to date and a page carries no changes. Set
  "Max Pages Per Sync" to bound very large initial syncs; the next run resumes from the saved cursor.

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
deployment takes many requests; the connector checkpoints state every 25 pages by default.

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
5. Choose a schema source. Pick "Inline JSON" if you want tables from components.

### Upgrading from 0.x

Version 1.0.0 is a breaking change:

- The connector reads from the Deployment API instead of the legacy `list_snapshot` and
  `document_deltas` endpoints, so it needs a Pro plan and a deploy key with `deployment:data:view`.
- State is a single Convex cursor stored per stream. Old 0.x state is ignored and the first sync starts from scratch.
- Component tables get their own streams named `<component>__<table>`.
- Records gain `_component` and `_table` fields.

Refresh the source schema and reset affected connections after upgrading.

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request                                             | Subject                                                          |
| :------ | :--------- | :------------------------------------------------------- | :--------------------------------------------------------------- |
| 1.0.0 | 2026-09-03 | [PR_NUMBER](https://github.com/paralov/airbyte/pull/PR_NUMBER) | Rewrite on the Convex Deployment API data sync endpoint: one resumable cursor, component tables as streams, inline schema option |
| 0.4.51 | 2025-10-07 | [65777](https://github.com/airbytehq/airbyte/pull/65777) | Update dependencies |
| 0.4.50 | 2025-08-23 | [65287](https://github.com/airbytehq/airbyte/pull/65287) | Update dependencies |
| 0.4.49 | 2025-08-16 | [65003](https://github.com/airbytehq/airbyte/pull/65003) | Update dependencies |
| 0.4.48 | 2025-08-09 | [64798](https://github.com/airbytehq/airbyte/pull/64798) | Update dependencies |
| 0.4.47 | 2025-07-19 | [63533](https://github.com/airbytehq/airbyte/pull/63533) | Update dependencies |
| 0.4.46 | 2025-07-12 | [62996](https://github.com/airbytehq/airbyte/pull/62996) | Update dependencies |
| 0.4.45 | 2025-07-05 | [62823](https://github.com/airbytehq/airbyte/pull/62823) | Update dependencies |
| 0.4.44 | 2025-06-28 | [62389](https://github.com/airbytehq/airbyte/pull/62389) | Update dependencies |
| 0.4.43 | 2025-06-21 | [61936](https://github.com/airbytehq/airbyte/pull/61936) | Update dependencies |
| 0.4.42 | 2025-06-14 | [61160](https://github.com/airbytehq/airbyte/pull/61160) | Update dependencies |
| 0.4.41 | 2025-05-24 | [60344](https://github.com/airbytehq/airbyte/pull/60344) | Update dependencies |
| 0.4.40 | 2025-05-10 | [59947](https://github.com/airbytehq/airbyte/pull/59947) | Update dependencies |
| 0.4.39 | 2025-05-03 | [59423](https://github.com/airbytehq/airbyte/pull/59423) | Update dependencies |
| 0.4.38 | 2025-04-26 | [58847](https://github.com/airbytehq/airbyte/pull/58847) | Update dependencies |
| 0.4.37 | 2025-04-19 | [58361](https://github.com/airbytehq/airbyte/pull/58361) | Update dependencies |
| 0.4.36 | 2025-04-12 | [57800](https://github.com/airbytehq/airbyte/pull/57800) | Update dependencies |
| 0.4.35 | 2025-04-05 | [57220](https://github.com/airbytehq/airbyte/pull/57220) | Update dependencies |
| 0.4.34 | 2025-03-29 | [55946](https://github.com/airbytehq/airbyte/pull/55946) | Update dependencies |
| 0.4.33 | 2025-03-08 | [55286](https://github.com/airbytehq/airbyte/pull/55286) | Update dependencies |
| 0.4.32 | 2025-03-01 | [54954](https://github.com/airbytehq/airbyte/pull/54954) | Update dependencies |
| 0.4.31 | 2025-02-22 | [54423](https://github.com/airbytehq/airbyte/pull/54423) | Update dependencies |
| 0.4.30 | 2025-02-15 | [53748](https://github.com/airbytehq/airbyte/pull/53748) | Update dependencies |
| 0.4.29 | 2025-02-08 | [52826](https://github.com/airbytehq/airbyte/pull/52826) | Update dependencies |
| 0.4.28 | 2025-01-25 | [52355](https://github.com/airbytehq/airbyte/pull/52355) | Update dependencies |
| 0.4.27 | 2025-01-18 | [51686](https://github.com/airbytehq/airbyte/pull/51686) | Update dependencies |
| 0.4.26 | 2025-01-11 | [51094](https://github.com/airbytehq/airbyte/pull/51094) | Update dependencies |
| 0.4.25 | 2024-12-28 | [50531](https://github.com/airbytehq/airbyte/pull/50531) | Update dependencies |
| 0.4.24 | 2024-12-21 | [50013](https://github.com/airbytehq/airbyte/pull/50013) | Update dependencies |
| 0.4.23 | 2024-12-14 | [49179](https://github.com/airbytehq/airbyte/pull/49179) | Update dependencies |
| 0.4.22 | 2024-11-25 | [48680](https://github.com/airbytehq/airbyte/pull/48680) | Starting with this version, the Docker image is now rootless. Please note that this and future versions will not be compatible with Airbyte versions earlier than 0.64 |
| 0.4.21 | 2024-10-29 | [47081](https://github.com/airbytehq/airbyte/pull/47081) | Update dependencies |
| 0.4.20 | 2024-10-12 | [46480](https://github.com/airbytehq/airbyte/pull/46480) | Update dependencies |
| 0.4.19 | 2024-09-28 | [46208](https://github.com/airbytehq/airbyte/pull/46208) | Update dependencies |
| 0.4.18 | 2024-09-21 | [45809](https://github.com/airbytehq/airbyte/pull/45809) | Update dependencies |
| 0.4.17 | 2024-09-14 | [45494](https://github.com/airbytehq/airbyte/pull/45494) | Update dependencies |
| 0.4.16 | 2024-09-07 | [45267](https://github.com/airbytehq/airbyte/pull/45267) | Update dependencies |
| 0.4.15 | 2024-08-31 | [45043](https://github.com/airbytehq/airbyte/pull/45043) | Update dependencies |
| 0.4.14 | 2024-08-24 | [44655](https://github.com/airbytehq/airbyte/pull/44655) | Update dependencies |
| 0.4.13 | 2024-08-17 | [44353](https://github.com/airbytehq/airbyte/pull/44353) | Update dependencies |
| 0.4.12 | 2024-08-10 | [43567](https://github.com/airbytehq/airbyte/pull/43567) | Update dependencies |
| 0.4.11 | 2024-08-03 | [43166](https://github.com/airbytehq/airbyte/pull/43166) | Update dependencies |
| 0.4.10 | 2024-07-27 | [42751](https://github.com/airbytehq/airbyte/pull/42751) | Update dependencies |
| 0.4.9 | 2024-07-20 | [42224](https://github.com/airbytehq/airbyte/pull/42224) | Update dependencies |
| 0.4.8 | 2024-07-13 | [41868](https://github.com/airbytehq/airbyte/pull/41868) | Update dependencies |
| 0.4.7 | 2024-07-10 | [41584](https://github.com/airbytehq/airbyte/pull/41584) | Update dependencies |
| 0.4.6 | 2024-07-09 | [41261](https://github.com/airbytehq/airbyte/pull/41261) | Update dependencies |
| 0.4.5 | 2024-07-06 | [40799](https://github.com/airbytehq/airbyte/pull/40799) | Update dependencies |
| 0.4.4 | 2024-06-25 | [40305](https://github.com/airbytehq/airbyte/pull/40305) | Update dependencies |
| 0.4.3 | 2024-06-22 | [40038](https://github.com/airbytehq/airbyte/pull/40038) | Update dependencies |
| 0.4.2 | 2024-06-06 | [39210](https://github.com/airbytehq/airbyte/pull/39210) | [autopull] Upgrade base image to v1.2.2 |
| 0.4.1 | 2024-05-21 | [38485](https://github.com/airbytehq/airbyte/pull/38485) | [autopull] base image + poetry + up_to_date |
| 0.4.0 | 2023-12-13 | [33431](https://github.com/airbytehq/airbyte/pull/33431) | 🐛 Convex source fix bug where full_refresh stops after one page |
| 0.3.0 | 2023-09-28 | [30853](https://github.com/airbytehq/airbyte/pull/30853) | 🐛 Convex source switch to clean JSON format |
| 0.2.0 | 2023-06-21 | [27226](https://github.com/airbytehq/airbyte/pull/27226) | 🐛 Convex source fix skipped records |
| 0.1.1 | 2023-03-06 | [23797](https://github.com/airbytehq/airbyte/pull/23797) | 🐛 Convex source connector error messages |
| 0.1.0 | 2022-10-24 | [18403](https://github.com/airbytehq/airbyte/pull/18403) | 🎉 New Source: Convex |

</details>
