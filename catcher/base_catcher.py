import logging
import time
from abc import ABC, abstractmethod
from threading import Event
from datetime import UTC, datetime

import botocore.client
import requests

logger = logging.getLogger(__name__)


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
    def extension(self) -> str:
        """File extension source is stored as."""
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
    def delay(self) -> int:
        """Seconds to delay between polls."""
        ...

    def __init__(
        self,
        s3_client: botocore.client.BaseClient,
        bucket: str,
        stop: Event
    ):
        """Store the variables specific to this poller.

        Args:
            s3_client: Initialized boto3 S3 client to use for accessing S3 bucket.
            bucket: S3 bucket identifier.
            stop: Threading Event to signal the thread to stop. 
        """

        self.s3_client = s3_client
        self.bucket = bucket
        self.stop = stop
        self.current_id = None

    def sleep(self) -> None:
        """Sleeps the thread for {delay} seconds.

        Replaces the functionality of time.sleep for thread objects.
        """

        self.stop.wait(self.delay)

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

    def fetch_object(self) -> requests.Response:
        """Fetches an object from the set url.

        Performs the GET with an If-None-Match header checking if the data served has changed since the last poll.
        Raises on a non 2xx status code and returns the response object.

        Returns:
            The response object.

        Raises:
            requests.RequestException: If the request fails, times out, or returns
            a 4xx or 5xx status.
        """

        headers = {
            'If-None-Match': self.current_id
        }
        response = requests.get(url=self.source_url, headers=headers, timeout=(self.connection_timeout, self.read_timeout))
        response.raise_for_status()
        return response

    def store_object(self, key: str, object: bytes) -> dict:
        """Uploads an object to S3 under a specified key.

        Args:
            key: Key to store the object in S3 as.
            object: Object bytes to store

        Returns:
            Response from the S3 put call.
        """

        return self.s3_client.put_object(
            Body=object,
            Bucket=self.bucket,
            Key=key,
            ContentType="application/octet-stream",
        )  

    def poll(self) -> None:
        """Polls the feed for new objects and stores them in S3.

        Checks the ETag header (File ID for Socrata) to determine when a new object has been served.
        When a new object has been served it's stored in S3.
        """

        while True:
            try:
                response = self.fetch_object()
                if response.status_code == 200: # New object served
                    logger.info("%s updated, processing new object", self.source_name)

                    object = response.content
                    timestamp = datetime.now(UTC)
                    logger.info("fetched %s object from %s", self.source_name, self.source_url)

                    key = self.build_key(timestamp=timestamp)
                    self.store_object(key=key, object=object)
                    logger.info("stored %s object from %s as: %s", self.source_name, self.source_url, key)

                    self.current_id = response.headers['ETag']
            except requests.ConnectTimeout:
                logger.error("connection timeout on %s from %s, retrying in %d seconds", self.source_name, self.source_url, self.delay)
            except requests.ReadTimeout:
                logger.error("read timeout on %s from %s, retrying in %d seconds", self.source_name, self.source_url, self.delay)
            except requests.RequestException:
                logger.error("request exception on %s from %s, retrying in %d seconds", self.source_name, self.source_url, self.delay, exc_info=True)
            except Exception:
                logger.error("non-request exception occurred on %s from %s, retrying in %d seconds", self.source_name, self.source_url, self.delay, exc_info=True)
            finally:
                self.sleep()

    def run(self) -> None:
        """Runs the catcher.
        """

        pass