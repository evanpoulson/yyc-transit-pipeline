"""Catcher for the vehicle_positions GTFS-RT feed."""

from catcher.base_catcher import Catcher


class VehiclePositionsCatcher(Catcher):
    source_name = "vehicle_positions"
    source_url = "https://data.calgary.ca/download/am7c-qe3u/application%2Foctet-stream"
    extension = "pb"
    connection_timeout = 5
    read_timeout = 10
    delay = 5
