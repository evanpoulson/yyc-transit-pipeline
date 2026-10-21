"""Bronze loader for the trip_updates GTFS-RT feed."""

import itertools
import logging
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import botocore.client
import botocore.exceptions
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

class TripUpdatesLoader():

    source_name = "trip_updates" # The name of the data source being loaded into bronze
    # The url of where the raw is being stored in S3
    source_url = "raw/trip_updates" 
    # The file extension raw is stored as in S3
    raw_extension = "pb"

    def __init__(
        self, 
        s3_client: botocore.client.BaseClient, 
        bucket: str, 
        thread_pool: ThreadPoolExecutor, 
        db: duckdb.DuckDBPyConnection
    ):
        """Store the variables specific to this loader.

        Args:
            s3_client: Initialized boto3 S3 client to use for accessing S3 bucket.
            bucket: S3 bucket identifier.
            thread_pool: Thread pool executor for concurrent operations.
            db: DuckDB engine connection to use for writing parquet.
        """
        
        self.s3_client = s3_client
        self.bucket = bucket
        self.thread_pool = thread_pool
        self.db = db

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
