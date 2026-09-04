# Convex

## Overview

Convex is the reactive backend-as-a-service for web developers.
As part of the backend, Convex stores developer-defined documents in tables, both in the
root of an app and inside installed [components](https://docs.convex.dev/components).

## Endpoints

The connector uses the [Convex Deployment API](https://docs.convex.dev/deployment-api/overview):

1. `POST /api/v1/data/sync` streams a resumable export of every selected table. Each page carries
   `truncates` (tables to drop), `values` (document upserts and tombstones, each tagged with its
   component path and table), a `status`, and an opaque `nextCursor`. One cursor covers all tables.
2. `GET /api/v1/data/list_active_syncs` is used as the connection check.

See [https://docs.convex.dev/deployment-api/data-sync](https://docs.convex.dev/deployment-api/data-sync).
