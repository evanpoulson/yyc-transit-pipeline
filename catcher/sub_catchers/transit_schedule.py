"""Catcher for the Calgary Transit Scheduling Data GTFS Static feed."""

from catcher.base_catcher import Catcher


class TransitScheduleCatcher(Catcher):
    source_name = "transit_schedule"
    source_url = (
        "https://data.calgary.ca/download/npk7-z3bj/application%2Fx-zip-compressed"
    )
    extension = "pb"
    connection_timeout = 300
    read_timeout = 600
    delay = 86400
