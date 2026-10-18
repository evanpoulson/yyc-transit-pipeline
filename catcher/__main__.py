"""Main script which runs and manages the project's various subcatchers.

Each subcatcher is run and maintained on its own thread.
"""

import logging
import threading
import time

import boto3

from catcher.sub_catchers import service_alerts, trip_updates, vehicle_positions

logger = logging.getLogger(__name__)

BUCKET = "yyc-transit-lake-860574615377-ca-central-1-an"
REGION = "ca-central-1"
HEALTH_CHECK_SECONDS = 10
SHUTDOWN_TIMEOUT_SECONDS = 20


def main() -> None:
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(message)s",
    )

    s3 = boto3.client("s3", region_name=REGION)
    stop = threading.Event()

    catchers = [
        service_alerts.ServiceAlertsCatcher(
            s3_client=s3, bucket=BUCKET, stop_event=stop
        ),
        trip_updates.TripUpdatesCatcher(s3_client=s3, bucket=BUCKET, stop_event=stop),
        vehicle_positions.VehiclePositionsCatcher(
            s3_client=s3, bucket=BUCKET, stop_event=stop
        ),
    ]

    threads = []
    for catcher in catchers:
        thread = threading.Thread(
            target=catcher.run, name=catcher.source_name, daemon=True
        )
        thread.start()
        logger.info(
            "%s started",
            catcher.source_name,
        )
        threads.append(thread)

    try:
        while not stop.is_set():
            for thread in threads:
                if not thread.is_alive():
                    # Exits so ECS can restart the task rather than restarting a thread.
                    # Exceptions are all handled, so a thread should only die in
                    # extremely unusual circumstances.
                    logger.error(
                        "%s died, exiting so ECS can restart the task", thread.name
                    )
                    raise SystemExit(1)
            stop.wait(HEALTH_CHECK_SECONDS)
    except KeyboardInterrupt:
        logger.info("stopping catchers...")
        stop.set()

        deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
        for thread in threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))


if __name__ == "__main__":
    main()
