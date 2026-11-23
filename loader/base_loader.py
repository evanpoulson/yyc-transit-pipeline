"""Base loader class that all source-specific bronze loaders subclass."""

import itertools
import logging
import os
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date
from typing import Literal

import botocore.client

logger = logging.getLogger(__name__)


class Loader(ABC):
    """Abstract base class for all source-specific bronze loaders.

    Provides shared S3 lakehouse I/O primitives—raw object discovery, single
    and bounded concurrent downloads, ``bronze``/``archive`` key generation,
    and multipart uploads—while requiring each subclass to declare its source
    configuration (``source_name``, ``source_path``, ``raw_extension``, and
    ``max_in_flight``) and implement ``run()``.
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Name of the source this loader targets, used in S3 keys and logs."""
        ...

    @property
    @abstractmethod
    def source_path(self) -> str:
        """S3 prefix where raw objects for this source are stored.

        Must include a trailing slash and no leading slash (for example,
        ``"raw/trip_updates/"``).
        """
        ...

    @property
    @abstractmethod
    def raw_extension(self) -> str:
        """File extension of raw objects, without a leading dot (for example, ``"pb"``)."""
        ...

    @property
    @abstractmethod
    def max_in_flight(self) -> int:
        """Maximum number of concurrent S3 downloads queued at once (must be ``>= 1``)."""
        ...

    def __init__(
        self,
        s3_client: botocore.client.BaseClient,
        bucket: str,
        thread_pool: ThreadPoolExecutor,
        target_date: date,
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

    def _get_raw_paths(self) -> list[str]:
        """List every raw object key for this source on the target date.

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

    def _get_raw_object(self, key: str) -> bytes:
        """Download one raw object and return its bytes.

        Args:
            key: The S3 object key.

        Returns:
            The object's raw bytes.

        Raises:
            botocore.exceptions.ClientError: Server side. If S3 rejects the download
                (for example, access denied or a missing bucket).
            botocore.exceptions.BotoCoreError: Client side. If the download never reaches
                S3 (for example, no credentials or a connection failure).
        """

        response = self.s3_client.get_object(
            Bucket=self.bucket,
            Key=key,
        )
        return response["Body"].read()

    def _iter_raw_objects(
        self, keys: list[str], max_in_flight: int | None = None
    ) -> Iterator[tuple[str, Future[bytes]]]:
        """Download raw objects concurrently using a bounded sliding window.

        Keeps at most ``max_in_flight`` S3 downloads pending at a time so raw
        bytes do not accumulate in memory faster than the caller can parse
        them, while preserving the input key order.

        Args:
            keys: Raw S3 object keys to download, in the order to yield them.
            max_in_flight: Maximum number of concurrent downloads to keep
                queued in the sliding window. Defaults to ``self.max_in_flight``.

        Yields:
            Tuples of ``(key, future)`` in the same order as ``keys``, where
            ``future`` resolves to the object's raw bytes (or raises a
            download exception when ``future.result()`` is called).

        Raises:
            ValueError: If ``max_in_flight`` is less than ``1``.
        """

        limit = self.max_in_flight if max_in_flight is None else max_in_flight
        if limit < 1:
            raise ValueError(f"max_in_flight must be >= 1, got {limit}")

        key_iter = iter(keys)
        pending: deque[tuple[str, Future[bytes]]] = deque()

        for key in itertools.islice(key_iter, limit):
            pending.append((key, self.thread_pool.submit(self._get_raw_object, key)))

        while pending:
            key, future = pending.popleft()
            next_key = next(key_iter, None)
            if next_key is not None:
                # Submit the replacement before yielding so S3 I/O overlaps with parsing.
                pending.append(
                    (next_key, self.thread_pool.submit(self._get_raw_object, next_key))
                )

            yield key, future

    def _build_key(
        self,
        layer: Literal["bronze", "archive"],
        table: str | None = None,
    ) -> str:
        """Build the S3 object key for a bronze table or raw archive on the target date.

        Produces a partition-friendly key scoped to the requested lakehouse
        layer:

        - ``bronze``: ``bronze/<source_name>/<table>/date=YYYY-MM-DD/<table>.parquet``
        - ``archive``: ``archive/<source_name>/date=YYYY-MM-DD/raw.tar.zst``

        Args:
            layer: Destination data lake layer (``"bronze"`` or ``"archive"``).
            table: Table name within the bronze layer (for example, ``"data"``
                or ``"capture_log"``). Required when ``layer == "bronze"``.

        Returns:
            The S3 object key with no leading slash or bucket name.

        Raises:
            ValueError: If ``layer`` is unsupported, or if ``layer == "bronze"``
                and ``table`` is not provided.
        """

        date_path = f"date={self.target_date.isoformat()}"
        if layer == "bronze":
            if not table:
                raise ValueError("table is required when layer is 'bronze'")
            return f"bronze/{self.source_name}/{table}/{date_path}/{table}.parquet"
        if layer == "archive":
            return f"archive/{self.source_name}/{date_path}/raw.tar.zst"
        raise ValueError(f"Unsupported layer: {layer!r}")

    def _upload_file(
        self,
        local_path: str,
        key: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Stream a local file to S3 under ``key`` using multipart upload.

        Args:
            local_path: Path to the local file to upload.
            key: Destination S3 object key (no bucket name or leading slash).
            content_type: MIME type stored on the S3 object.

        Raises:
            boto3.exceptions.S3UploadFailedError: If S3 rejects or drops the
                upload.
            FileNotFoundError: If ``local_path`` does not exist.
        """

        self.s3_client.upload_file(
            Filename=local_path,
            Bucket=self.bucket,
            Key=key,
            ExtraArgs={"ContentType": content_type},
        )
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        logger.info(
            "%s: uploaded %.2f MB to s3://%s/%s",
            self.source_name,
            size_mb,
            self.bucket,
            key,
        )

    @abstractmethod
    def run(self) -> None:
        """Load one day of raw objects for this source into the bronze and archive layers."""
        ...
