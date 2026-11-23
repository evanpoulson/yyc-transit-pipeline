"""Base loader class that all GTFS-RT bronze loaders subclass."""

import io
import logging
import resource
import sys
import tarfile
import time
from abc import abstractmethod
from collections import Counter
from tempfile import TemporaryDirectory
from typing import Literal

import botocore.exceptions
import pyarrow as pa
import pyarrow.parquet as pq
from base_loader import Loader
from google.protobuf import message
from google.transit import gtfs_realtime_pb2
from schemas import SCHEMAS

logger = logging.getLogger(__name__)


class GtfsRtLoader(Loader):
    """Abstract base class for GTFS-RT protobuf bronze loaders.

    Implements the shared real-time bronze load pipeline: streams daily
    ``.pb`` snapshots through ``Loader``'s bounded sliding window, archives
    unmodified bytes into a daily ``raw.tar.zst`` tarball, validates and
    decodes each ``FeedMessage`` via subclass-defined ``_append_entity()``,
    flushes typed Arrow batches to Parquet in ``row_group_size`` chunks, and
    records per-snapshot outcomes in ``capture_log``.

    Subclasses must define ``source_name``, ``source_path``, ``max_in_flight``,
    ``data_schema``, ``row_group_size``, and ``_append_entity()``.
    """

    @property
    @abstractmethod
    def data_schema(self) -> pa.Schema:
        """Explicit PyArrow schema for this feed's bronze ``data`` table."""
        ...

    @property
    @abstractmethod
    def row_group_size(self) -> int:
        """Target row count per flushed Parquet row group."""
        ...

    @property
    def raw_extension(self) -> str:
        """File extension of raw GTFS-RT protobuf snapshots (``"pb"``)."""
        return "pb"

    @property
    def capture_log_schema(self) -> pa.Schema:
        """Explicit PyArrow schema for the shared ``capture_log`` audit table."""
        return SCHEMAS["capture_log"]

    @abstractmethod
    def _append_entity(
        self,
        entity: gtfs_realtime_pb2.FeedEntity,
        header_timestamp: int | None,
        buf: dict[str, list],
    ) -> None:
        """Flatten one GTFS-RT ``FeedEntity`` and append its rows to ``buf``.

        Subclasses extract the feed-specific message from ``entity`` (skipping
        entities that do not carry it) and append values in place to every
        column list in ``buf``, keeping all column lists equal in length.

        Args:
            entity: One ``FeedEntity`` from a parsed snapshot.
            header_timestamp: The containing snapshot's ``FeedHeader``
                timestamp (POSIX seconds, UTC), or ``None`` if omitted.
            buf: Columnar buffer (``dict[str, list]``) keyed by
                ``self.data_schema.names`` to append rows into in place.
        """
        ...

    @staticmethod
    def _new_buffer(schema: pa.Schema) -> dict[str, list]:
        """Create an empty columnar buffer matching ``schema``.

        Args:
            schema: Explicit PyArrow schema whose column names become the
                buffer keys.

        Returns:
            Mapping of each column name in ``schema`` to a fresh empty list.
        """

        buf = {column: [] for column in schema.names}
        return buf

    def _parse_snapshot(self, snapshot: bytes) -> tuple[int | None, pa.RecordBatch]:
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

        # Proto2 ParseFromString accepts wire-boundary truncations; verify required fields.
        if not feed.IsInitialized():
            raise message.DecodeError(
                f"incomplete FeedMessage, missing {feed.FindInitializationErrors()}"
            )

        header_timestamp = (
            feed.header.timestamp if feed.header.HasField("timestamp") else None
        )

        buf = self._new_buffer(schema=self.data_schema)
        for entity in feed.entity:
            self._append_entity(entity, header_timestamp, buf)

        batch = pa.RecordBatch.from_pydict(buf, schema=self.data_schema)
        return header_timestamp, batch

    @staticmethod
    def _flush_batch(
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

    @staticmethod
    def _append_capture_log(
        capture_log: dict[str, list],
        key: str,
        status: Literal["ok", "download_failed", "parse_failed"],
        header_timestamp: int | None = None,
        byte_size: int | None = None,
        row_count: int | None = None,
    ) -> None:
        """Append one snapshot-level outcome row to the ``capture_log`` buffer.

        Extracts ``capture_timestamp`` from the raw object key's
        ``<source>_<epoch>.<ext>`` filename suffix (storing ``None`` if the key
        does not match that format) and appends all six ``capture_log`` columns
        in place.

        Args:
            capture_log: Columnar buffer keyed by ``self.capture_log_schema.names``.
            key: Raw S3 object key for the snapshot.
            status: Outcome for this snapshot (``"ok"``, ``"download_failed"``,
                or ``"parse_failed"``).
            header_timestamp: Snapshot ``FeedHeader`` timestamp (POSIX seconds,
                UTC), or ``None`` if unavailable or omitted.
            byte_size: Raw payload size in bytes, or ``None`` if the download
                failed.
            row_count: Number of decoded rows produced by the snapshot, or
                ``None`` if downloading or parsing failed.
        """

        filename = key.rsplit("/", 1)[-1]
        stem = filename.rsplit(".", 1)[0]
        try:
            capture_timestamp: int | None = int(stem.rsplit("_", 1)[1])
        except IndexError, ValueError:
            capture_timestamp = None

        capture_log["object_key"].append(key)
        capture_log["capture_timestamp"].append(capture_timestamp)
        capture_log["header_timestamp"].append(header_timestamp)
        capture_log["status"].append(status)
        capture_log["byte_size"].append(byte_size)
        capture_log["row_count"].append(row_count)

    def run(self) -> None:
        """Load one day of raw snapshots into the bronze and archive layers on S3.

        Lists the target date's raw objects, streams them concurrently through a
        bounded sliding window, archives raw bytes into a daily ``.tar.zst``
        tarball, parses each snapshot into a typed Arrow ``RecordBatch``,
        flushes accumulated batches to Parquet in ``row_group_size`` chunks,
        records per-snapshot outcomes in ``capture_log``, and uploads produced
        files to S3.

        Upload rules when partial or total failures occur:
            - ``capture_log.parquet`` is always uploaded when keys are found so
              failed days still leave a queryable audit trail.
            - ``data.parquet`` is skipped if ``0`` rows were decoded (avoiding
              overwriting a good partition with an empty file).
            - ``raw.tar.zst`` is skipped if ``0`` objects downloaded.

        Raises:
            RuntimeError: If no raw object keys are discovered for the target
                date.
        """

        start_time = time.monotonic()
        logger.info(
            "%s: starting bronze load for %s",
            self.source_name,
            self.target_date,
        )

        keys = self._get_raw_paths()
        if not keys:
            raise RuntimeError(
                f"No raw keys discovered for {self.source_name} on {self.target_date}."
            )
        logger.info(
            "%s: discovered %d raw objects for %s",
            self.source_name,
            len(keys),
            self.target_date,
        )

        with TemporaryDirectory() as tmpdir:
            data_path = f"{tmpdir}/data.parquet"
            capture_log_path = f"{tmpdir}/capture_log.parquet"
            archive_path = f"{tmpdir}/raw.tar.zst"

            with (
                pq.ParquetWriter(
                    data_path, self.data_schema, compression="zstd"
                ) as writer,
                tarfile.open(archive_path, "w:zst") as archive,
            ):
                pending_batches: list[pa.RecordBatch] = []
                capture_log = self._new_buffer(schema=self.capture_log_schema)
                pending_rows = 0
                total_rows = 0
                archived_count = 0

                for key, future in self._iter_raw_objects(keys=keys):
                    try:
                        blob = future.result()
                    except (
                        botocore.exceptions.ClientError,
                        botocore.exceptions.BotoCoreError,
                    ):
                        logger.warning(
                            "%s: download failed for %s",
                            self.source_name,
                            key,
                            exc_info=True,
                        )
                        self._append_capture_log(
                            capture_log=capture_log,
                            key=key,
                            status="download_failed",
                        )
                        continue

                    # Archive raw bytes before parsing so corrupt payloads stay recoverable.
                    tarinfo = tarfile.TarInfo(name=key)
                    tarinfo.size = len(blob)
                    archive.addfile(tarinfo=tarinfo, fileobj=io.BytesIO(blob))
                    archived_count += 1

                    try:
                        header_ts, batch = self._parse_snapshot(blob)
                    except message.DecodeError:
                        # Only catch upstream protobuf corruption; let Arrow schema errors fail loudly.
                        logger.debug(
                            "%s: parse failed for %s",
                            self.source_name,
                            key,
                            exc_info=True,
                        )
                        self._append_capture_log(
                            capture_log=capture_log,
                            key=key,
                            status="parse_failed",
                            byte_size=len(blob),
                        )
                        continue

                    self._append_capture_log(
                        capture_log=capture_log,
                        key=key,
                        status="ok",
                        header_timestamp=header_ts,
                        byte_size=len(blob),
                        row_count=batch.num_rows,
                    )

                    # Skip 0-row batches so pending_batches stays non-empty and quiet feeds don't bloat it.
                    if batch.num_rows > 0:
                        pending_batches.append(batch)
                        pending_rows += batch.num_rows
                        total_rows += batch.num_rows

                    if pending_rows >= self.row_group_size:
                        self._flush_batch(
                            writer=writer,
                            batches=pending_batches,
                            schema=self.data_schema,
                        )
                        pending_rows = 0

                if pending_batches:
                    self._flush_batch(
                        writer=writer,
                        batches=pending_batches,
                        schema=self.data_schema,
                    )

            # Writers are now closed (Parquet footer and zstd frames flushed) while tmpdir is still live.
            # Always upload capture_log so even an all-failed day leaves an audit trail.
            table = pa.Table.from_pydict(capture_log, schema=self.capture_log_schema)
            pq.write_table(table, capture_log_path, compression="zstd")
            self._upload_file(
                local_path=capture_log_path,
                key=self._build_key(layer="bronze", table="capture_log"),
            )

            if total_rows > 0:
                self._upload_file(
                    local_path=data_path,
                    key=self._build_key(layer="bronze", table="data"),
                )
            else:
                logger.warning(
                    "%s: skipping data upload for %s (0 rows decoded)",
                    self.source_name,
                    self.target_date,
                )

            if archived_count > 0:
                self._upload_file(
                    local_path=archive_path,
                    key=self._build_key(layer="archive"),
                    content_type="application/zstd",
                )
            else:
                logger.warning(
                    "%s: skipping archive upload for %s (0 objects downloaded)",
                    self.source_name,
                    self.target_date,
                )

        elapsed = time.monotonic() - start_time
        # ru_maxrss is reported in bytes on macOS and KiB on Linux/Fargate.
        ru_maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_rss_mb = (
            ru_maxrss / (1024 * 1024) if sys.platform == "darwin" else ru_maxrss / 1024
        )
        status_counts = Counter(capture_log["status"])
        logger.info(
            "%s: finished bronze load for %s in %.2fs "
            "(discovered=%d, ok=%d, download_failed=%d, parse_failed=%d, "
            "rows=%d, archived=%d, peak_rss=%.1fMB)",
            self.source_name,
            self.target_date,
            elapsed,
            len(keys),
            status_counts["ok"],
            status_counts["download_failed"],
            status_counts["parse_failed"],
            total_rows,
            archived_count,
            peak_rss_mb,
        )
