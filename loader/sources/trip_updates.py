"""Bronze loader for the trip_updates GTFS-RT feed."""

import io
import itertools
import tarfile
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date
from tempfile import TemporaryDirectory

import botocore.client
import botocore.exceptions
import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

from schemas import SCHEMAS

class TripUpdatesLoader():

    source_name = "trip_updates" # The name of the data source being loaded into bronze
    # The path of where the raw is being stored in S3
    source_path = "raw/trip_updates/" 
    # The file extension raw is stored as in S3
    raw_extension = "pb"
    # The pyarrow schema for this data table this loader produces.
    data_schema = SCHEMAS[source_name]
    # The size of a row group for this loaders data file.
    row_group_size = 250_000

    def __init__(
        self, 
        s3_client: botocore.client.BaseClient, 
        bucket: str, 
        thread_pool: ThreadPoolExecutor, 
        target_date: date
    ):
        """Store the variables specific to this loader.

        Args:
            s3_client: Initialized boto3 S3 client to use for accessing S3 bucket.
            bucket: S3 bucket identifier.
            thread_pool: Thread pool executor for concurrent operations.
            target_date: Date to run the bronze load on.
        """
        
        self.s3_client = s3_client
        self.bucket = bucket
        self.thread_pool = thread_pool
        self.target_date = target_date

    def get_raw_paths(self) -> list[str]:
        """List every raw object key for this feed on the target date.

        Lists objects under ``<source_path><YYYY>/<MM>/<DD>/`` and filters to
        keys ending with ``.<raw_extension>`` so directory markers and stray
        files are excluded while empty captures are preserved.

        Returns:
            All matching raw object keys for the target date in chronological
            (lexicographical) order, possibly empty.
        """

        date_path = self.target_date.strftime("%Y/%m/%d")
        prefix = f"{self.source_path}{date_path}/"
        suffix = f".{self.raw_extension}"

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        paths = []
        for page in page_iterator:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(suffix):
                    paths.append(key)

        return paths

    def get_raw_object(self, key: str) -> bytes:
        """Download one raw object and return its bytes.

        Args:
            key: The S3 object key.

        Returns:
            The object's raw bytes.

        Raises:
            botocore.exceptions.ClientError: Server side. If S3 rejects the download 
                (for example, access denied or a missing bucket).
            botocore.exceptions.BotoCoreError: Client Side. If the download never reaches 
                S3 (for example, no credentials or a connection failure).
        """

        response = self.s3_client.get_object(
            Bucket=self.bucket,
            Key=key,
        )
        return response["Body"].read()

    def iter_raw_objects(
        self, keys: list[str], max_in_flight: int = 16
    ) -> Iterator[tuple[str, Future[bytes]]]:
        """Download raw objects concurrently using a bounded sliding window.

        Keeps at most ``max_in_flight`` S3 downloads pending at a time so raw
        bytes do not accumulate in memory faster than the caller can parse
        them, while preserving the input key order.

        Args:
            keys: Raw S3 object keys to download, in the order to yield them.
            max_in_flight: Maximum number of concurrent downloads to keep
                queued in the sliding window.

        Yields:
            Tuples of ``(key, future)`` in the same order as ``keys``, where
            ``future`` resolves to the object's raw bytes (or raises a
            download exception when ``future.result()`` is called).
        """

        key_iter = iter(keys)
        pending = deque()

        # Pre-load the deque 
        for key in itertools.islice(key_iter, max_in_flight):
            pending.append((key, self.thread_pool.submit(self.get_raw_object, key)))

        while pending:
            key, future = pending.popleft()
            next_key = next(key_iter, None)
            if next_key is not None:
                pending.append((next_key, self.thread_pool.submit(self.get_raw_object, next_key)))

            yield key, future

    def new_buffer(self) -> dict[str, list]:
        """Create an empty columnar buffer matching ``data_schema``.

        Returns:
            Mapping of each column name in ``self.data_schema`` to a fresh
            empty list, ready to collect decoded rows before flushing to a
            ``pyarrow.RecordBatch``.
        """

        buf = {column: [] for column in self.data_schema.names}
        return buf

    def _append_entity(
        self,
        entity: gtfs_realtime_pb2.FeedEntity,
        header_timestamp: int | None,
        buf: dict[str, list],
    ) -> None:
        """Flatten one ``TripUpdate`` entity and append its rows to ``buf``.

        Explodes each ``TripUpdate`` into one row per ``stop_time_update``, or
        one row with null stop columns if there are none so the trip is not
        dropped. Entities without a ``trip_update`` field are skipped. Optional
        protobuf fields are read through ``HasField`` on their owning message
        so absent values stay ``None`` rather than protobuf defaults.

        Args:
            entity: One ``FeedEntity`` from a parsed snapshot.
            header_timestamp: The containing snapshot's ``FeedHeader``
                timestamp (POSIX seconds, UTC), or ``None`` if omitted.
            buf: Columnar buffer (``dict[str, list]``) keyed by
                ``self.data_schema.names`` to append rows into in place.
        """

        if not entity.HasField("trip_update"):
            return

        tu = entity.trip_update
        t = tu.trip if tu.HasField("trip") else None
        d = tu.vehicle if tu.HasField("vehicle") else None

        entity_id = entity.id or None
        trip_id = t.trip_id if t and t.HasField("trip_id") else None
        route_id = t.route_id if t and t.HasField("route_id") else None
        direction_id = t.direction_id if t and t.HasField("direction_id") else None
        start_time = t.start_time if t and t.HasField("start_time") else None
        start_date = t.start_date if t and t.HasField("start_date") else None
        schedule_relationship = (
            t.schedule_relationship if t and t.HasField("schedule_relationship") else None
        )
        vehicle_id = d.id if d and d.HasField("id") else None
        vehicle_label = d.label if d and d.HasField("label") else None
        timestamp = tu.timestamp if tu.HasField("timestamp") else None
        delay = tu.delay if tu.HasField("delay") else None

        stops = tu.stop_time_update
        if not stops:
            buf["header_timestamp"].append(header_timestamp)
            buf["entity_id"].append(entity_id)
            buf["trip_id"].append(trip_id)
            buf["route_id"].append(route_id)
            buf["direction_id"].append(direction_id)
            buf["start_time"].append(start_time)
            buf["start_date"].append(start_date)
            buf["schedule_relationship"].append(schedule_relationship)
            buf["vehicle_id"].append(vehicle_id)
            buf["vehicle_label"].append(vehicle_label)
            buf["timestamp"].append(timestamp)
            buf["delay"].append(delay)

            buf["stop_sequence"].append(None)
            buf["stop_id"].append(None)
            buf["arrival_delay"].append(None)
            buf["arrival_time"].append(None)
            buf["departure_delay"].append(None)
            buf["departure_time"].append(None)
            buf["departure_occupancy_status"].append(None)
            buf["stop_schedule_relationship"].append(None)
            return

        n = len(stops)
        buf["header_timestamp"].extend([header_timestamp] * n)
        buf["entity_id"].extend([entity_id] * n)
        buf["trip_id"].extend([trip_id] * n)
        buf["route_id"].extend([route_id] * n)
        buf["direction_id"].extend([direction_id] * n)
        buf["start_time"].extend([start_time] * n)
        buf["start_date"].extend([start_date] * n)
        buf["schedule_relationship"].extend([schedule_relationship] * n)
        buf["vehicle_id"].extend([vehicle_id] * n)
        buf["vehicle_label"].extend([vehicle_label] * n)
        buf["timestamp"].extend([timestamp] * n)
        buf["delay"].extend([delay] * n)

        for u in stops:
            arr = u.arrival if u.HasField("arrival") else None
            dep = u.departure if u.HasField("departure") else None

            buf["stop_sequence"].append(
                u.stop_sequence if u.HasField("stop_sequence") else None
            )
            buf["stop_id"].append(u.stop_id if u.HasField("stop_id") else None)
            buf["arrival_delay"].append(
                arr.delay if arr and arr.HasField("delay") else None
            )
            buf["arrival_time"].append(
                arr.time if arr and arr.HasField("time") else None
            )
            buf["departure_delay"].append(
                dep.delay if dep and dep.HasField("delay") else None
            )
            buf["departure_time"].append(
                dep.time if dep and dep.HasField("time") else None
            )
            buf["departure_occupancy_status"].append(
                u.departure_occupancy_status
                if u.HasField("departure_occupancy_status")
                else None
            )
            buf["stop_schedule_relationship"].append(
                u.schedule_relationship if u.HasField("schedule_relationship") else None
            )

    def parse_snapshot(self, snapshot: bytes) -> tuple[int | None, pa.RecordBatch]:
        """Parse one raw GTFS-RT snapshot into a typed Arrow ``RecordBatch``.

        Validates the ``FeedMessage``, reads the ``FeedHeader`` timestamp once
        and copies it onto every row, and converts the decoded columns into a
        ``pyarrow.RecordBatch`` typed by ``self.data_schema``. Because each
        snapshot is parsed and schema-checked in isolation, a corrupt or
        incompatible payload fails without affecting any previously accumulated
        batches.

        Args:
            snapshot: Raw protobuf bytes of one ``FeedMessage``.

        Returns:
            A ``(header_timestamp, batch)`` tuple, where ``header_timestamp``
            is the snapshot's POSIX UTC timestamp (or ``None`` if omitted) and
            ``batch`` is a ``pyarrow.RecordBatch`` conforming to
            ``self.data_schema`` (possibly with ``0`` rows on an empty feed).

        Raises:
            google.protobuf.message.DecodeError: If the bytes are not a valid,
                fully-initialized ``FeedMessage``.
            pyarrow.ArrowInvalid: If decoded column lengths or values violate
                ``self.data_schema``.
            pyarrow.ArrowTypeError: If a decoded value cannot be cast to its
                declared column type in ``self.data_schema``.
        """

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(snapshot)

        if not feed.IsInitialized():
            raise message.DecodeError(
                f"incomplete FeedMessage, missing {feed.FindInitializationErrors()}"
            )

        header_timestamp = (
            feed.header.timestamp if feed.header.HasField("timestamp") else None
        )

        buf = self.new_buffer()
        for entity in feed.entity:
            self._append_entity(entity, header_timestamp, buf)

        batch = pa.RecordBatch.from_pydict(buf, schema=self.data_schema)
        return header_timestamp, batch

    def flush_batch(
        self,
        writer: pq.ParquetWriter,
        batches: list[pa.RecordBatch],
        schema: pa.Schema,
    ) -> None:
        """Write accumulated Arrow batches as one Parquet row group and clear them.

        Combines ``batches`` into a zero-copy ``pyarrow.Table`` using ``schema``,
        appends it to ``writer`` as a single row group, and clears ``batches`` in
        place so the buffered Arrow memory can be released immediately.

        Args:
            writer: Open ``ParquetWriter`` receiving the row group.
            batches: Non-empty list of ``RecordBatch`` objects to flush; cleared
                in place after writing.
            schema: Explicit PyArrow schema matching ``writer`` and ``batches``.
        """

        table = pa.Table.from_batches(batches, schema=schema)
        writer.write_table(table)
        batches.clear()

    def build_bronze_key(self, table: str) -> str:
        """Builds the S3 key for a bronze object on the target date.

        Produces a partition-friendly, unique key of the form
        ``bronze/<source_name>/<table>/date=YYYY-MM-DD/<table>.parquet``.

        Args:
            table: Table the file will be stored as (capture_log or data).

        Returns:
            The S3 key string with no leading slash or bucket name.
        """

        date_path = f"date={self.target_date.isoformat()}"
        key = f"bronze/{self.source_name}/{table}/{date_path}/{table}.parquet"
        return key

    def run(self) -> None:
        """Load one day of raw snapshots into the bronze layer on S3.

        Lists the target date's raw objects, streams them concurrently through a
        bounded sliding window, parses each snapshot into a typed Arrow
        ``RecordBatch``, flushes accumulated batches to a local Parquet file in
        ``row_group_size`` chunks, and uploads the finished Parquet file to S3.

        Raises:
            RuntimeError: If no raw object keys are discovered for the target
                date.
        """

        keys = self.get_raw_paths()
        if not keys:
            raise RuntimeError(
                f"No raw keys discovered for {self.source_name} on {self.target_date}."
            )
        
        with TemporaryDirectory() as tmpdir:
            data_path = f"{tmpdir}/data.parquet"
            capture_log_path = f"{tmpdir}/capture_log.parquet"
            archive_path = f"{tmpdir}/raw.tar.zst"

            with (
                pq.ParquetWriter(data_path, self.data_schema) as writer,
                #TODO: instantiate capture log collector
                tarfile.open(archive_path, "w:zst") as archive,
            ):

                pending_batches: list[pa.RecordBatch] = []
                pending_rows = 0
                total_rows = 0
                for key, future in self.iter_raw_objects(keys=keys, max_in_flight=16):

                    try:
                        blob = future.result()

                        tarinfo = tarfile.TarInfo(name=key)
                        tarinfo.size = len(blob)
                        archive.addfile(tarinfo=tarinfo, fileobj=io.BytesIO(blob))
                        
                        header_ts, batch = self.parse_snapshot(blob)
                    except botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError:
                        #TODO: capture log and logger log
                        continue
                    except message.DecodeError:
                        #TODO: capture log and logger log
                        continue

                    #TODO: add a row to the capture log for the snapshot

                    if batch.num_rows > 0:
                        pending_batches.append(batch)
                        pending_rows += batch.num_rows
                        total_rows += batch.num_rows

                    if pending_rows >= self.row_group_size:
                        self.flush_batch(writer=writer, batches=pending_batches, schema=self.data_schema)
                        pending_rows = 0

                if pending_batches:
                    self.flush_batch(writer=writer, batches=pending_batches, schema=self.data_schema)

            #TODO: upload capture log to s3
            if total_rows > 0:
                self.s3_client.upload_file(
                    Filename=data_path,    
                    Bucket=self.bucket,
                    Key=self.build_bronze_key("data"),
                    ExtraArgs={"ContentType": "application/octet-stream"}, 
                )

        #TODO: log for completeness
