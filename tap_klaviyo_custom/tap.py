"""klaviyo-custom tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_klaviyo_custom.streams import (
    RESTStream,
    ListMembersStream,
    ListsStream
)

STREAM_TYPES = [
    ListMembersStream,
    ListsStream
]


class Tapklaviyo_custom(Tap):
    """klaviyo-custom tap class."""
    name = "klaviyo_custom"

    config_jsonschema = th.PropertiesList(
        th.Property("auth_token", th.StringType, required=False),
        th.Property("start_date", th.DateTimeType),
        th.Property("api_url", th.StringType, default="https://a.klaviyo.com/api/v2/"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
