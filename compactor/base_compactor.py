from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import defaultdict
import itertools
import logging

import botocore.client
import botocore.exceptions
import duckdb
import pyarrow as pa
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


class Compactor(ABC):

    # fraction of snapshots allowed to fail before the run refuses to write
    max_failure_rate: float = 0.01

    @property
    @abstractmethod
    def feed_name(self) -> str: ...

    @property
    @abstractmethod
    def schema(self) -> pa.Schema: ...

    @property
    @abstractmethod
    def sort_keys(self) -> tuple[str, ...]: ...

    def __init__(
        self,
        s3_client: botocore.client.BaseClient,
        bucket: str,
        executor: ThreadPoolExecutor,
        db: duckdb.DuckDBPyConnection,
    ):
        self.s3_client = s3_client
        self.bucket = bucket
        self.executor = executor
        self.db = db

        self.attempted = 0
        self.succeeded = 0
        self.download_failed = 0
        self.parse_failed = 0

    @abstractmethod
    def shape_entity(self, entity: gtfs_realtime_pb2.FeedEntity) -> list[dict]: ...

    def reset_counters(self) -> None:
        self.attempted = 0
        self.succeeded = 0
        self.download_failed = 0
        self.parse_failed = 0

    @property
    def failed(self) -> int:
        return self.download_failed + self.parse_failed

    @property
    def failure_rate(self) -> float:
        if self.attempted == 0:
            return 0.0
        return self.failed / self.attempted

    def build_prefix(self, layer: str, feed: str, target_day: datetime) -> str:

        if layer == "raw":
            date_path = target_day.strftime("%Y/%m/%d")
            return f"raw/{feed}/{date_path}/"
        else:
            day = target_day.strftime("%Y-%m-%d")
            return f"curated/{feed}/date={day}/data.parquet"

    def group_by_hour(self, paths: list[str]) -> dict[str, list[str]]:
        hours = defaultdict(list)
        for key in paths:
            hours[key.rsplit("/", 1)[0]].append(key)
        return hours

    def get_object_paths(self, target_day: datetime) -> list[str]:

        prefix = self.build_prefix(layer="raw", feed=self.feed_name, target_day=target_day)

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        paths = []
        for page in page_iterator:

            # 'Contents' won't exist if the prefix or folder is completely empty
            if "Contents" in page:
                for obj in page["Contents"]:
                    paths.append(obj["Key"])

        logger.info("%s: found %d raw objects under %s", self.feed_name, len(paths), prefix)
        return paths

    def get_object(self, key: str) -> bytes:

        response = self.s3_client.get_object(
            Bucket=self.bucket,
            Key=key,
        )
        return response["Body"].read()

    def parse_snapshot(self, snapshot: bytes) -> list[dict]:

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(snapshot)
        entities = [self.shape_entity(entity) for entity in feed.entity]
        return list(itertools.chain.from_iterable(entities))

    def fetch_rows(self, paths: list[str]) -> list[dict]:

        self.attempted += len(paths)

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

        order_by = ", ".join(self.sort_keys)
        key = self.build_prefix(layer="curated", feed=self.feed_name, target_day=target_day)
        write_path = f"s3://{self.bucket}/{key}"

        # the view name below must match the name used in the FROM clause
        self.db.register("df", df)
        try:
            self.db.execute(
                f"""
                COPY (
                    SELECT DISTINCT * FROM df
                    ORDER BY {order_by}
                ) TO ? (FORMAT parquet);
                """,
                [write_path],
            )
        finally:
            self.db.unregister("df")

        logger.info("%s: wrote %s", self.feed_name, write_path)

    def validate_sort_keys(self) -> None:
        missing = [key for key in self.sort_keys if key not in self.schema.names]
        if missing:
            raise RuntimeError(
                f"{self.feed_name}: sort keys {missing} are not columns in the schema"
            )

    def run(self, day: datetime) -> None:

        self.validate_sort_keys()
        self.reset_counters()

        logger.info("%s: starting compaction for %s", self.feed_name, day.strftime("%Y-%m-%d"))

        table = self.create_table(day)

        logger.info(
            "%s: %d/%d snapshots ok, %d download failures, %d parse failures (%.2f%% failed)",
            self.feed_name,
            self.succeeded,
            self.attempted,
            self.download_failed,
            self.parse_failed,
            self.failure_rate * 100,
        )

        if self.failure_rate > self.max_failure_rate:
            raise RuntimeError(
                f"{self.feed_name}: failure rate {self.failure_rate:.2%} exceeds "
                f"threshold {self.max_failure_rate:.2%}, refusing to write a partial partition"
            )

        self.write_curated(table, day)
        logger.info("%s: done", self.feed_name)