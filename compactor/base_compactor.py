"""Base class for the jobs that compact raw snapshots into curated Parquet.

One `Compactor` subclass exists per feed. The base class owns everything that
is identical across feeds: listing and downloading a day's raw snapshots from
S3, parsing and flattening them concurrently, batching by hour to bound memory,
accounting for download and parse failures, and writing one sorted,
deduplicated, typed Parquet file for the day with its completeness and quality
metrics stamped into the footer. A subclass supplies only what differs: the
feed name, the PyArrow schema, the sort keys, and `shape_entity`, which turns
one protobuf entity into one or more flat rows.
"""

import itertools
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import botocore.client
import botocore.exceptions
import duckdb
import pyarrow as pa
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

import config

logger = logging.getLogger(__name__)


class Compactor(ABC):
    """Compacts one day of one feed's raw snapshots into a curated Parquet file.

    Subclasses define `feed_name`, `schema`, `sort_keys`, and `shape_entity`.
    One instance is used per feed per run; it is not thread-safe, and its
    counters are reset at the start of each `run`.

    Counters accumulated over a run:
        discovered:      raw objects found in S3 for the day.
        succeeded:       snapshots downloaded and parsed without error.
        download_failed: snapshots that could not be fetched from S3.
        parse_failed:    snapshots fetched but not parseable as a FeedMessage.
    """

    @property
    @abstractmethod
    def feed_name(self) -> str:
        """Short feed identifier, matching config.FEEDS and the S3 key prefix."""
        ...

    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        """PyArrow schema for this feed's curated rows."""
        ...

    @property
    @abstractmethod
    def sort_keys(self) -> tuple[str, ...]:
        """Columns to sort the curated file by, for compression and row-group skipping."""
        ...

    def __init__(
        self,
        s3_client: botocore.client.BaseClient,
        bucket: str,
        executor: ThreadPoolExecutor,
        db: duckdb.DuckDBPyConnection,
    ):
        """Store the shared resources a run needs and zero the counters.

        Args:
            s3_client: Initialized boto3 S3 client, shared across feeds.
            bucket: Name of the S3 bucket holding both raw and curated data.
            executor: Shared thread pool the per-snapshot downloads run on.
            db: Open DuckDB connection with an S3 secret already configured.
        """
        self.s3_client = s3_client
        self.bucket = bucket
        self.executor = executor
        self.db = db

        self.expected = config.EXPECTED_SNAPSHOTS
        self.discovered = 0
        self.succeeded = 0
        self.download_failed = 0
        self.parse_failed = 0

    @abstractmethod
    def shape_entity(self, entity: gtfs_realtime_pb2.FeedEntity) -> list[dict]:
        """Flatten one protobuf entity into a list of curated row dicts.

        Returns a list because the row grain is the leaf, not the entity: a
        trip update with many stop updates, or an alert with many informed
        entities, becomes many rows. An entity with no leaves still returns one
        row so it is not dropped. Optional fields are read through HasField on
        their owning message, so an absent field is None rather than the
        protobuf default (an unreported bearing must not read as due north).
        """
        ...

    def reset_counters(self) -> None:
        """Zero the per-run counters. Called at the start of each run."""
        self.discovered = 0
        self.succeeded = 0
        self.download_failed = 0
        self.parse_failed = 0

    @property
    def failed(self) -> int:
        """Total failed snapshots, download plus parse."""
        return self.download_failed + self.parse_failed

    @property
    def failure_rate(self) -> float:
        """Fraction of discovered snapshots that failed to download or parse, in [0, 1]."""
        if self.discovered == 0:
            return 0.0
        return self.failed / self.discovered

    @property
    def coverage(self) -> float:
        """Fraction of the day's expected snapshots that were discovered, in [0, 1]."""
        if self.discovered == 0:
            return 0.0
        return self.discovered / self.expected

    def build_prefix(self, layer: str, feed: str, target_day: datetime) -> str:
        """Build the S3 key prefix (raw) or full object key (curated) for a feed and day.

        Args:
            layer: "raw" for the day's snapshot prefix; anything else for the
                curated partition's data.parquet key.
            feed: Short feed identifier.
            target_day: The day to build the path for.

        Returns:
            An S3 key or prefix, with no bucket and no leading slash.
        """
        if layer == "raw":
            date_path = target_day.strftime("%Y/%m/%d")
            return f"raw/{feed}/{date_path}/"
        else:
            day = target_day.strftime("%Y-%m-%d")
            return f"curated/{feed}/date={day}/data.parquet"

    def group_by_hour(self, paths: list[str]) -> dict[str, list[str]]:
        """Group raw object keys by their hour prefix.

        Keys are grouped on everything up to the last "/", which is the
        raw/<feed>/<Y>/<M>/<D>/<H> hour directory, so the day can be processed
        one hour at a time to bound memory.

        Args:
            paths: Raw object keys for a day.

        Returns:
            Mapping of hour prefix to the keys under it.
        """
        hours = defaultdict(list)
        for key in paths:
            hours[key.rsplit("/", 1)[0]].append(key)
        return hours

    def get_object_paths(self, target_day: datetime) -> list[str]:
        """List every raw object key for this feed on the target day.

        Args:
            target_day: The day to list.

        Returns:
            All raw object keys under the day's prefix, possibly empty.
        """
        prefix = self.build_prefix(layer="raw", feed=self.feed_name, target_day=target_day)

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        paths = []
        for page in page_iterator:
            # 'Contents' is absent when a page (or the whole prefix) is empty.
            if "Contents" in page:
                for obj in page["Contents"]:
                    paths.append(obj["Key"])

        logger.info("%s: found %d raw objects under %s", self.feed_name, len(paths), prefix)
        return paths

    def get_object(self, key: str) -> bytes:
        """Download one raw object and return its bytes.

        Args:
            key: The S3 object key.

        Returns:
            The object's raw bytes.

        Raises:
            botocore.exceptions.ClientError: If the object cannot be fetched.
        """
        response = self.s3_client.get_object(
            Bucket=self.bucket,
            Key=key,
        )
        return response["Body"].read()

    def parse_snapshot(self, snapshot: bytes) -> list[dict]:
        """Parse one raw snapshot and flatten its entities into rows.

        Args:
            snapshot: Raw protobuf bytes of one FeedMessage.

        Returns:
            The flattened rows from every entity in the snapshot.

        Raises:
            google.protobuf.message.DecodeError: If the bytes are not a valid,
                fully-initialized FeedMessage (truncated or corrupt).
        """
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(snapshot)

        if not feed.IsInitialized():
            raise message.DecodeError(
                f"incomplete FeedMessage, missing {feed.FindInitializationErrors()}"
            )

        entities = [self.shape_entity(entity) for entity in feed.entity]
        return list(itertools.chain.from_iterable(entities))

    def fetch_rows(self, paths: list[str]) -> list[dict]:
        """Download and parse a batch of snapshots concurrently, returning rows.

        Dispatches every key to the shared thread pool, since per-object S3
        GETs are latency-bound. Download and parse failures are counted
        separately and skipped, so one bad snapshot does not sink the batch;
        the counts feed the run's coverage and failure metrics.

        Args:
            paths: Raw object keys to fetch (typically one hour's worth).

        Returns:
            Flattened rows from every snapshot that downloaded and parsed.
        """
        self.discovered += len(paths)

        rows = []
        futures = {self.executor.submit(self.get_object, key): key for key in paths}

        for future in as_completed(futures):
            object_key = futures[future]

            try:
                blob = future.result()
            except botocore.exceptions.ClientError:
                self.download_failed += 1
                logger.warning("%s: download failed for %s", self.feed_name, object_key, exc_info=True)
                continue
            except Exception:
                self.download_failed += 1
                logger.warning("%s: unexpected download error for %s", self.feed_name, object_key, exc_info=True)
                continue

            try:
                data = self.parse_snapshot(blob)
            except Exception:
                self.parse_failed += 1
                logger.warning("%s: parse failed for %s", self.feed_name, object_key, exc_info=True)
                continue

            rows.extend(data)
            self.succeeded += 1
            logger.debug("%s: read %s (%d rows)", self.feed_name, object_key, len(data))

        return rows

    def create_table(self, target_day: datetime) -> pa.Table:
        """Build one PyArrow table for the day, one hour at a time.

        Lists the day's keys, groups them by hour, and fetches and flattens
        each hour before moving on, so at most one hour of Python dicts is live
        at once (a whole day of trip updates is tens of millions of rows and
        will not fit in memory otherwise). Each hour becomes a compact Arrow
        table immediately, and the hourly tables are concatenated at the end.

        Args:
            target_day: The day to compact.

        Returns:
            One Arrow table for the whole day, typed by self.schema.

        Raises:
            RuntimeError: If no raw snapshots were found, so a broken or empty
                run fails loudly rather than overwriting a good partition with
                an empty file.
        """
        tables = []

        paths = self.get_object_paths(target_day)
        paths_by_hour = self.group_by_hour(paths)

        for hour, keys in sorted(paths_by_hour.items()):
            rows = self.fetch_rows(keys)
            tables.append(pa.Table.from_pylist(rows, schema=self.schema))
            logger.info("%s: hour %s produced %d rows", self.feed_name, hour, len(rows))

        if not tables:
            raise RuntimeError(
                f"no raw snapshots found for {self.feed_name} on {target_day:%Y-%m-%d}"
            )

        table = pa.concat_tables(tables)
        logger.info("%s: built table with %d rows", self.feed_name, table.num_rows)
        return table

    def write_curated(self, df: pa.Table, target_day: datetime) -> None:
        """Write the day's table to S3 as one sorted, deduplicated Parquet file.

        DuckDB writes straight to S3 through httpfs. Rows are deduplicated with
        SELECT DISTINCT (consecutive polls of an unrefreshed feed produce
        genuinely identical rows) and sorted by the feed's sort keys, which is
        free while DuckDB is already sorting and improves Parquet compression
        and row-group skipping. The run's completeness and quality metrics are
        embedded in the Parquet footer as key-value metadata, so each file is
        self-describing: they are read back with parquet_kv_metadata and cannot
        drift from the data they describe.

        Args:
            df: The day's table from create_table.
            target_day: The day being written, used for the partition path.
        """
        order_by = ", ".join(self.sort_keys)
        key = self.build_prefix(layer="curated", feed=self.feed_name, target_day=target_day)
        write_path = f"s3://{self.bucket}/{key}"

        # DuckDB reads the table through a registered view; the name here must
        # match the one used in the FROM clause below.
        self.db.register("df", df)
        try:
            self.db.execute(
                f"""
                COPY (SELECT DISTINCT * FROM df ORDER BY {order_by})
                TO '{write_path}' (FORMAT parquet, KV_METADATA {{
                    build_ts: '{datetime.now(timezone.utc).isoformat()}',
                    feed_name: '{self.feed_name}',
                    day: '{target_day.strftime("%Y-%m-%d")}',
                    expected: '{self.expected}',
                    discovered: '{self.discovered}',
                    succeeded: '{self.succeeded}',
                    download_failed: '{self.download_failed}',
                    parse_failed: '{self.parse_failed}',
                    coverage: '{self.coverage:.4f}',
                    failure_rate: '{self.failure_rate:.4f}'
                }});
                """
            )
        finally:
            self.db.unregister("df")

        logger.info("%s: wrote %s", self.feed_name, write_path)

    def validate_sort_keys(self) -> None:
        """Fail fast if a declared sort key is not a column in the schema.

        A cheap guard run before any downloading, so a typo in sort_keys is
        caught immediately rather than after a whole day has been fetched.

        Raises:
            RuntimeError: If any sort key is missing from the schema.
        """
        missing = [key for key in self.sort_keys if key not in self.schema.names]
        if missing:
            raise RuntimeError(
                f"{self.feed_name}: sort keys {missing} are not columns in the schema"
            )

    def run(self, day: datetime) -> None:
        """Compact one day of this feed, end to end.

        Validates the sort keys, resets counters, builds the day's table, logs
        the coverage and failure metrics, and writes the curated file. There is
        deliberately no failure-rate gate: the metrics are recorded in the
        footer and the decision to trust a partition is left to downstream. A
        day that yields no snapshots still fails in create_table rather than
        writing an empty file over a good partition.

        Args:
            day: The UTC day to compact.
        """
        self.validate_sort_keys()
        self.reset_counters()

        logger.info("%s: starting compaction for %s", self.feed_name, day.strftime("%Y-%m-%d"))

        table = self.create_table(day)

        logger.info(
            "%s: %.2f%% snapshot coverage, %.2f%% snapshots failed (%d download failures, %d parse failures)",
            self.feed_name,
            self.coverage * 100,
            self.failure_rate * 100,
            self.download_failed,
            self.parse_failed,
        )

        self.write_curated(table, day)
        logger.info("Successfully finished compacting %s", self.feed_name)
