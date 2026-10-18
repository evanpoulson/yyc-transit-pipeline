"""Main script which runs and manages the projects various subcatchers.

Each subcatcher is ran and maintained on it's own personal thread.
"""

import logging
import threading

import boto3

import config

logger = logging.getLogger(__name__)

BUCKET = "yyc-transit-lake-860574615377-ca-central-1-an"
REGION = "ca-central-1"


def main() -> None:

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(message)s",
    )

    s3 = boto3.client("s3", region_name=REGION)


if __name__ == "__main__":
    main()
