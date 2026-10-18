"""Catcher for the trip_updates GTFS-RT feed."""

from catcher.base_catcher import Catcher


class TripUpdatesCatcher(Catcher):
    source_name = "trip_updates"
    source_url = "https://data.calgary.ca/download/gs4m-mdc2/application%2Foctet-stream"
    extension = "pb"
    connection_timeout = 5
    read_timeout = 10
    delay = 5
