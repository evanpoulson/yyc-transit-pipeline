"""Bronze loader for the trip_updates GTFS-RT feed."""

import itertools
import logging
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import botocore.client
import botocore.exceptions
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

class TripUpdatesLoader():

    source_name = "trip_updates" # The name of the data source being loaded into bronze
    # The path of where the raw is being stored in S3
    source_path = "raw/trip_updates/" 
    # The file extension raw is stored as in S3
    raw_extension = "pb"

    def __init__(
        self, 
        s3_client: botocore.client.BaseClient, 
        bucket: str, 
        thread_pool: ThreadPoolExecutor, 
        db: duckdb.DuckDBPyConnection,
        target_date: date
    ):
        """Store the variables specific to this loader.

        Args:
            s3_client: Initialized boto3 S3 client to use for accessing S3 bucket.
            bucket: S3 bucket identifier.
            thread_pool: Thread pool executor for concurrent operations.
            db: DuckDB engine connection to use for writing parquet.
            target_date: Date to run the bronze load on.
        """
        
        self.s3_client = s3_client
        self.bucket = bucket
        self.thread_pool = thread_pool
        self.db = db
        self.target_date = target_date

    def get_raw_paths(self) -> list[str]:
        """List every raw object key for this feed on the target day.

        Args:
            target_day: The day to list.

        Returns:
            All raw object keys under the day's prefix, possibly empty.
        """

        date_path = self.target_date.strftime("%Y/%m/%d")
        prefix = f"raw/{self.source_name}/{date_path}/"

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        paths = []
        for page in page_iterator:
            # 'Contents' is absent when a page (or the whole prefix) is empty.
            if "Contents" in page:
                for obj in page["Contents"]:
                    paths.append(obj["Key"])

        return paths

    def group_raw_paths(self, paths: list[str]) -> dict[str, list[str]]:
        """Group raw object keys by their hour prefix.

        Keys are grouped by the hourso the day can be processed
        one hour at a time to bound memory.

        Args:
            paths: Raw object keys for a day.

        Returns:
            Mapping of hour prefix to the keys under it.
        """
        hours = defaultdict(list)
        for key in paths:
            hour = key.split(sep="/")[-2]
            hours[hour].append(key)
        return dict(hours)

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

    def build_bronze_key(self, table: str) -> str:
        """Builds the S3 key for a bronze object at a provided time.

        Produces a partition-friendly, unique key of the form
        bronze/<source_name>/<table>/<date=xxxx-xx-xx/<table>.parquet

        Args:
            table: Table the file will be stored as (capture_log or data).

        Returns:
            The S3 key string with no leading slash or bucket name.
        """

        date_path = f"date={self.target_date.isoformat()}"
        key = f"bronze/{self.source_name}/{table}/{date_path}/{table}.parquet"
        return key

    def store_bronze_object(self, key: str, data: bytes) -> dict:
        """Uploads a bronze object to S3 under the provided key.

        Args:
            key: Key to store the object in S3 as.
            data: Bytes to store.

        Returns:
            Response from the S3 put call.

        Raises:
            botocore.exceptions.ClientError: If S3 rejects the upload (for example,
                access denied or a missing bucket).
            botocore.exceptions.BotoCoreError: If the upload never reaches S3
                (for example, no credentials or a connection failure).
        """

        return self.s3_client.put_object(
            Body=data,
            Bucket=self.bucket,
            Key=key,
            ContentType="application/octet-stream",
        )
