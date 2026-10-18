"""Base catcher class that all source-specific catchers subclass."""

import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from threading import Event

import botocore.client
import botocore.exceptions
import requests

logger = logging.getLogger(__name__)


class Catcher(ABC):
    """Base class for all catchers polling data sources for this project.

    Each catcher defines its own 'source_name', 'source_url', 'extension',
    'connection_timeout', 'read_timeout', and 'delay' to ensure specificity
    to that data source. 
    
    Only one instance of a catcher is used for a particular data source.
    """

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
    def extension(self) -> str:
        """File extension source is stored as."""
        ...

    @property
    @abstractmethod
    def connection_timeout(self) -> int:
        """Seconds to wait for source connection before timing out."""
        ...

    @property
    @abstractmethod
    def read_timeout(self) -> int:
        """Seconds to wait between bytes before timing out."""
        ...

    @property
    @abstractmethod
    def delay(self) -> int:
        """Seconds to delay between polls."""
        ...

    def __init__(
        self, s3_client: botocore.client.BaseClient, bucket: str, stop_event: Event
    ):
        """Store the variables specific to this poller.

        Args:
            s3_client: Initialized boto3 S3 client to use for accessing S3 bucket.
            bucket: S3 bucket identifier.
            stop_event: Threading Event to signal the thread to stop.
        """

        self.s3_client = s3_client
        self.bucket = bucket
        self.stop_event = stop_event
        self.current_id: str | None = None
        # Reuses the connection for all requests resulting in less load on source and faster polls.
        self.session = requests.Session()

    def sleep(self) -> None:
        """Sleeps the thread for {delay} seconds.

        Returns early when the flag is set.
        """

        self.stop_event.wait(self.delay)

    def build_key(self, timestamp: datetime) -> str:
        """Builds the S3 bucket object key for a particular object at a particular time.

        Produces a partition-friendly, unique key of the form
        raw/<source_name>/<YYYY>/<MM>/<DD>/<HH>/<source_name>_<epoch>.<extension>

        Args:
            timestamp: Timestamp of when the object was obtained.

        Returns:
            The S3 key string with no leading slash or bucket name.
        """

        date_path = timestamp.strftime("%Y/%m/%d/%H")
        epoch = int(timestamp.timestamp())
        key = f"raw/{self.source_name}/{date_path}/{self.source_name}_{epoch}.{self.extension}"
        return key

    def fetch_response(self) -> requests.Response:
        """Fetches a response from the set url.

        Performs the GET with an If-None-Match header checking if the data served has changed since the last poll.
        Raises on a non 4xx or 5xx status code and returns the response object.

        Returns:
            The response object.

        Raises:
            requests.RequestException: If the request fails, times out, or returns
                a 4xx or 5xx status.
        """

        # Use If-None-Match header rather than head so snapshots come in the same request
        headers = {"If-None-Match": self.current_id}
        response = self.session.get(
            url=self.source_url,
            headers=headers,
            timeout=(self.connection_timeout, self.read_timeout),
        )
        response.raise_for_status()
        return response

    def store_object(self, key: str, data: bytes) -> dict:
        """Uploads an object to S3 under a specified key.

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

    def poll_once(self) -> None:
        """Polls the feed for new objects and stores them in S3.

        Checks the ETag header (File ID for Socrata) to determine when a new object has been served.
        When a new object has been served it's stored in S3.
        Only updates the current_id after the upload succeeds.

        Raises:
            requests.RequestException: If the request fails, times out, or returns
                a 4xx or 5xx status.
            botocore.exceptions.ClientError: If S3 rejects the upload (for example,
                access denied or a missing bucket).
            botocore.exceptions.BotoCoreError: If the upload never reaches S3
                (for example, no credentials or a connection failure).
            KeyError: If a 200 response has no ETag header.
        """

        response = self.fetch_response()
        if response.status_code == 200:  # New object served
            data = response.content
            timestamp = datetime.now(UTC)
            logger.info(
                "%s updated, fetched new object from %s",
                self.source_name,
                self.source_url,
            )

            key = self.build_key(timestamp=timestamp)
            self.store_object(key=key, data=data)
            logger.info(
                "stored %s object from %s as: %s",
                self.source_name,
                self.source_url,
                key,
            )

            self.current_id = response.headers["ETag"]

    def run(self) -> None:
        """Runs the catcher.

        Calls poll_once() and sleeps {delay} seconds between each successful or failed poll.
        Catches and logs exceptions when raised to prevent thread disruption.
        """

        while not self.stop_event.is_set():
            try:
                self.poll_once()
            except requests.ConnectTimeout:
                logger.warning(
                    "connection timeout occurred on %s from %s, retrying in %d seconds",
                    self.source_name,
                    self.source_url,
                    self.delay,
                )
            except requests.ReadTimeout:
                logger.warning(
                    "read timeout occurred on %s from %s, retrying in %d seconds",
                    self.source_name,
                    self.source_url,
                    self.delay,
                )
            except requests.RequestException as err:
                logger.exception(
                    "request exception occurred on %s from %s: %s, retrying in %d seconds",
                    self.source_name,
                    self.source_url,
                    err,
                    self.delay,
                )
            except botocore.exceptions.ClientError as err:
                logger.exception(
                    "S3 rejected upload of %s to %s: %s, retrying in %d seconds",
                    self.source_name,
                    self.bucket,
                    err,
                    self.delay,
                )
            except botocore.exceptions.BotoCoreError as err:
                logger.exception(
                    "could not upload %s to S3: %s, retrying in %d seconds",
                    self.source_name,
                    err,
                    self.delay,
                )
            except Exception as err:
                logger.exception(
                    "unexpected exception occurred on %s from %s: %s, retrying in %d seconds",
                    self.source_name,
                    self.source_url,
                    err,
                    self.delay,
                )
            finally:
                self.sleep()
