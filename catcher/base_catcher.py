import argparse
import logging
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import boto3
import requests
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

import config

class Catcher(ABC):

    @property
    @abstractmethod
    def source_url(self) -> str:
        """The source url this catcher is ingesting."""
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

    