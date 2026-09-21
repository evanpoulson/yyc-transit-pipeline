# tests

Pytest suite for the pipeline. The layout mirrors the packages, one folder per
component:

```
tests/
  catcher/     tests for the ingestion service
  compactor/   tests for the curation job (planned: shape_entity per feed)
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
exercised without touching the network or S3. When the compactor tests land,
`shape_entity` is the highest-value target: it is pure, and a saved snapshot
with a field unset is the regression case for the presence-check class of bug.
