import logging
import time 
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, Future
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
        """Seconds to delay before retrying a request."""
        ...

    def __init__(
        self,
        s3_client: botocore.client.BaseClient,
        bucket: str,
        executor: ThreadPoolExecutor
    ):
        """Store the variables specific to this poller.

        Args:
            s3_client: Initialized boto3 S3 client to use for accessing S3 bucket.
            bucket: S3 bucket identifier.
            executor: Thread pool to run on.
        """

        self.s3_client = s3_client
        self.bucket = bucket
        self.executor = executor
        self.current_id = None

    def build_key(self, source_name: str, extension: str, timestamp: datetime) -> str:
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

    def fetch_object(self, url: str, connect_timeout: int, read_timeout: str) -> requests.Response:
        """Fetches an object from a provided url.

        Does no validation. Simply performs the GET, raises on a non 2xx status code,
        and returns the raw bytes of the object. Parsing and errors are handled downstream.

        Args:
            url: The url of the data source.
            connect_timeout: Time to wait to connect before timing out.
            read_tiemout: Time to wait between bytes before timing out.

        Returns:
            The response object.

        Raises:
            requests.RequestException: If the request fails, times out, or returns
            a 4xx or 5xx status.
        """

        headers = {
            'If-None-Match': self.current_id
        }
        object = requests.get(url=url, headers=headers, timeout=(connect_timeout, read_timeout))
        object.raise_for_status()
        return object

    def store_object(self, s3_client: botocore.client.BaseClient, bucket: str, key: str, object: bytes) -> dict:
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

    def poll(self) -> None:
        """Polls the feed for new objects and stores them in S3.

        Checks the ETag header (File ID for Socrata) to determine when a new object has been served.
        When a new object has been served it stored it in S3.
        """

        while True:
            try:
                response = self.fetch_object(url=self.source_url, connect_timeout=self.connection_timeout, read_timeout=self.read_timeout)
                if response.status_code == 200: # New object served
                    logger.info("%s updated, processing new object", self.source_name)

                    object = response.content
                    timestamp = datetime.now(UTC)
                    logger.info("fetched %s object from %s", self.source_name, self.source_url)

                    key = self.build_key(source_name=self.source_name, extension=self.extension, timestamp=timestamp)
                    self.store_object(s3_client=self.s3_client, bucket=self.bucket, key=key, object=object)
                    logger.info("stored %s object from %s as: %s", self.source_name, self.source_url, key)

                    self.current_id = response.headers['ETag']

                time.sleep(self.delay)
            except requests.ConnectTimeout:
                logger.error("connection timeout on %s from %s, retrying in %d", self.source_name, self.source_url, self.connection_timeout)
                time.sleep(self.connection_timeout)
            except requests.ReadTimeout:
                logger.error("read timeout on %s from %s, retrying in %d", self.source_name, self.source_url, self.read_timeout)
                time.sleep(self.read_timeout)
            except requests.RequestException:
                logger.error("request exception on %s from %s", self.source_name, self.source_url, exc_info=True)
                time.sleep(self.delay)

    def run(self) -> Future:
        """Runs the catcher.
        
        Uses the thread pool it was constructed with.

        Returns:
            The future the poll service is running on.
        """

        return self.executor.submit(self.poll)