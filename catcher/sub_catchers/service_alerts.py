"""Catcher for the service_alerts GTFS-RT feed."""

from catcher.base_catcher import Catcher


class ServiceAlertsCatcher(Catcher):
    source_name = "service_alerts"
    source_url = "https://data.calgary.ca/download/jhgn-ynqj/application%2Foctet-stream"
    extension = "pb"
    connection_timeout = 5
    read_timeout = 10
    delay = 5
