"""klaviyo tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_klaviyo_custom.streams import ListMembersStream, ListsStream

STREAM_TYPES = [ListMembersStream, ListsStream]


class TapKlaviyo(Tap):
    """Klaviyo tap class."""

    name = "klaviyo"

    config_jsonschema = th.PropertiesList(
        th.Property("api_url", th.StringType, default="https://a.klaviyo.com/api/v2/"),
        th.Property("api_key", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType),
        th.Property("list_ids", th.ArrayType(th.StringType), required=True),
        th.Property("user_agent", th.StringType, default="fixdauto/tap-klaviyo-custom"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
