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
        date_path = timestamp.strftime("%Y/%m/%d/%H")
        epoch = int(timestamp.timestamp())
        key = f"raw/{source_name}/{date_path}/{source_name}_{epoch}.{extension}"
        return key

    