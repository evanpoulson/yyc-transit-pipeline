import argparse
import logging
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import boto3
import botocore.client
import requests
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

import config

class Catcher(ABC):

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Name of the source this catcher is ingesting."""
        ...

    @property
    @abstractmethod
    def source_url(self) -> str:
        """Source url this catcher is ingesting."""
        ...

    @property
    @abstractmethod
    def connection_timeout(self) -> int:
        """Time to wait for source connection before timing out."""
        ...

    @property
    @abstractmethod
    def read_timeout(self) -> int:
        """Time to wait between bytes before timing out."""
        ...

    @property
    @abstractmethod
    def s3_client(self) -> botocore.client.BaseClient:
        """S3 client to use for storing objects."""
        ...

    @property
    @abstractmethod
    def bucket(self) -> str:
        """S3 bucket the objects are uploaded to."""
        ...

    def build_key(source_name: str, extension: str, timestamp: datetime) -> str:
        """Builds the S3 bucket object key for a particular object at a particular time.

        Produces a partition-friendly, unique key of the form
        raw/<source_name>/<YYYY>/<MM>/<DD>/<HH>/<source_name>_<epoch>.<extension>

        Args:
            source_name: Short identifier for the data source. 
            extension: The file extension for the object to be stored as.
            timestamp: Timestamp of when the object was obtained.

        Returns:
            The S3 key string with no leading slash or bucket name.
        """

        date_path = timestamp.strftime("%Y/%m/%d/%H")
        epoch = int(timestamp.timestamp())
        key = f"raw/{source_name}/{date_path}/{source_name}_{epoch}.{extension}"
        return key

    def fetch_object(url: str, connect_timeout: int, read_timeout: str) -> bytes:
        """Fetches an object from a provided url.

        Does no validation. Simply performs the GET, raises on a non 2xx status code,
        and returns the raw bytes of the object. Parsing and errors are handled downstream.

        Args:
            url: The url of the data source.
            connect_timeout: Time to wait to connect before timing out.
            read_tiemout: Time to wait between bytes before timing out.

        Returns:
            The raw bytes of the fetched object.
        """

        object = requests.get(url, timeout=(connect_timeout, read_timeout))
        object.raise_for_status
        return object.content

    def store_object(s3_client: botocore.client.BaseClient, bucket: str, key: str, object: bytes) -> dict:
        """Uploads an object to S3 under a specified key.

        Args:
            s3_client: An initialized boto3 client.
            bucket: The S3 to store the object in.
            key: Key to store the object in S3 as.
            object: Object bytes to store

        Returns:
            Response from the S3 put call.
        """

        return s3_client.put_object(
            Body=object,
            Bucket=bucket,
            Key=key,
            ContentType="application/octet-stream",
        )