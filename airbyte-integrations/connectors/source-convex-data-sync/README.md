# Convex Data Sync source connector

Exports root and component tables through Convex's Deployment API. See the
[connector documentation](../../../docs/integrations/sources/convex-data-sync.md)
for configuration, output fields, and recovery limitations.

## Development

Use Python 3.10–3.12 and Poetry. From this directory:

```sh
poetry install --with dev
poetry run pytest unit_tests
```

Create `secrets/config.json` using `integration_tests/sample_config.json` as a
starting point. Replace the URL, deploy key, and table schemas with those of your
Convex Pro deployment. The `secrets` directory is gitignored.

```sh
poetry run source-convex-data-sync spec
poetry run source-convex-data-sync check --config secrets/config.json
poetry run source-convex-data-sync discover --config secrets/config.json
poetry run source-convex-data-sync read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

The sample catalog selects root `posts` and `betterAuth/user`. Replace it with a
configured catalog for your deployment before running a read.

## Live integration test

Use a disposable Convex deployment (Pro on Convex Cloud) with a seeded root table and
one seeded component table. Select only those fixtures in the config's table
schemas. The test reads those tables, validates their records, and resumes from
the returned state. It does not mutate the deployment.

```sh
CONVEX_TEST_CONFIG=/absolute/path/to/config.json poetry run pytest integration_tests
```

Without that variable the test looks for `secrets/config.json`, and skips if it
is absent. Unit tests use mocked HTTP responses; passing them does not verify
live Convex behavior or destination deduplication.

## Docker image

From this directory, with Docker running and `yq` installed:

```sh
docker build --platform linux/amd64 -f ../../../docker-images/Dockerfile.python-connector \
  --build-arg CONNECTOR_NAME=source-convex-data-sync \
  --build-arg BASE_IMAGE="$(yq -r '.data.connectorBuildOptions.baseImage' metadata.yaml)" \
  -t airbyte/source-convex-data-sync:dev .
docker run --rm --platform linux/amd64 airbyte/source-convex-data-sync:dev spec
```
