# tests

Pytest suite for the pipeline. The layout mirrors the packages, one folder per
component:

```
tests/
  catcher/     tests for the ingestion service
  compactor/   tests for the curation job
```

Run from the repo root:

```
python -m pytest
```

`pytest.ini` puts the repo root on the path, so tests import `catcher`,
`compactor`, and `config` exactly as the services do at runtime. Install the
dev dependencies first with `pip install -r requirements-dev.txt`.

The catcher tests build real GTFS-RT payloads and drive the fetch loop with a
mocked `fetch_once`, so the retry, passthrough, and logging behaviour is
exercised without touching the network or S3.

The compactor tests build real protobuf entities and flatten them through
`shape_entity`, so the presence checks (an absent field is None, an explicit
zero is kept) are covered directly, along with the schema-to-shape consistency,
the coverage and failure-rate metrics, the concurrent `fetch_rows` failure
counting (via a small FakeS3), and the empty-day guard in `create_table`. A
saved snapshot with a field unset is the exact regression case for the
presence-check class of bug.

Both suites are mutation-checked: deliberately breaking the behaviour they
describe (dropping the catcher's timeout, leaking a protobuf default past a
presence check, removing the empty-day guard) makes them fail, so they catch a
regression rather than just passing.
