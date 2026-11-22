"""Bronze loader for the trip_updates GTFS-RT feed."""

import itertools
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date

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
    ) -> int:
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

        Returns:
            Number of rows appended to ``buf`` for this entity.
        """

        if not entity.HasField("trip_update"):
            return 0

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
            return 1

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

        return n

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

    def run(self):
        """"""

        keys = self.get_raw_paths()

        # might want to chunk this into a:
        # with tempfile.TemporaryDirectory()
        with pq.ParquetWriter(f"/tmp/{self.source_name}", self.data_schema):

            buf = self.new_buffer()
            for key, future in self.iter_raw_objects(keys=keys, max_in_flight=16):
                pass
                # try block to read a future for the snapshot bytes

                # if we get em then decode the snapshot
                # add the snapshot as a row to our buffer
                # also add a row to the capture log for the snapshot

                # check if the buffer length is >= 250,000
                    # if so flush it with pa.RecordBatch
                    # then clear it to free the memory

                # rinse repeat

            # if the buffer still has rows (len > 0)
            # then flush one more time 

        # here we want to store the file in s3 with s3.upload_file
        # at the same time we're also going to want to upload the capture log
        # here we're going to want to store the full parquet file to s3
