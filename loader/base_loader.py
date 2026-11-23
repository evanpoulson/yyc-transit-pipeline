"""Base loader class that all source-specific bronze loaders subclass."""

import io
import itertools
import logging
import os
import resource
import sys
import tarfile
import time
from abc import ABC, abstractmethod
from collections import Counter, deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date
from tempfile import TemporaryDirectory
from typing import Literal

import botocore.client
import botocore.exceptions
import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import message
from google.transit import gtfs_realtime_pb2
from schemas import SCHEMAS

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)


class Catcher(ABC):
    """
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Name of the source this loader targets."""
        ...

    @property
    @abstractmethod
    def source_path(self) -> str:
        """Source path this loader targets."""
        ...

    @property
    @abstractmethod
    def raw_extension(self) -> str:
        """File extension source is stored as."""
        ...

    @property
    @abstractmethod
    def data_schema(self) -> pa.Schema:
        """Data table schema for this loader."""
        ...

    @property
    @abstractmethod
    def capture_log_schema(self) -> pa.Schema:
        """Capture log schema for this loader."""
        ...

    @property
    @abstractmethod
    def row_group_size(self) -> int:
        """Row group size for the parquet data table."""
        ...

    @property
    @abstractmethod
    def max_in_flight(self) -> int:
        """Upper bound on concurrent downloads at a time."""
        ...
