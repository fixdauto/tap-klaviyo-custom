"""Stream type classes for tap-klaviyo-custom."""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from singer_sdk.authenticators import APIAuthenticatorBase, APIKeyAuthenticator
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class KlaviyoStream(RESTStream):
    """Base class for TapKlaviyo streams."""

    records_jsonpath = "$[*]"

    @property
    def url_base(self) -> str:
        """Return the base url, e.g. ``https://api.mysite.com/v3/``."""
        return self.config["api_url"]

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        """Return or set the authenticator for managing HTTP auth headers."""
        return APIKeyAuthenticator(
            stream=self,
            key="api_key",
            value=self.config["api_key"],
            location="params",
        )

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        By default, checks for error status codes (>400) and raises a
        :class:`singer_sdk.exceptions.FatalAPIError`.

        Raises
        ------
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        """
        if response.status_code == 429:
            wait_time = int(response.headers["Retry-After"])
            self.logger.info(f"Throttled. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            raise RetriableAPIError("Throttled")

        # Otherwise use default error checking
        super().validate_response(response)


class ListsStream(KlaviyoStream):
    """A stream for lists."""

    name = "lists"
    path = "lists"
    primary_keys = ["list_id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "lists.json"

    @property
    def partitions(self) -> Optional[List[dict]]:
        """Get stream partitions."""
        return [{"list_id": id} for id in self.config["list_ids"]]

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        """Return token identifying next page or None if all records have been read."""
        # since we're using partitions, there's only ever one page per partition
        return None

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """As needed, append or transform raw data to match expected structure."""
        # For some reason,  the id is not included in the response
        assert context is not None
        return {**row, "list_id": context["list_id"]}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        assert context is not None
        return {"list_id": context["list_id"]}


class ListMembersStream(KlaviyoStream):
    """A Stream for list members."""

    name = "list_members"
    path = "group/{list_id}/members/all"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "list_members.json"
    records_jsonpath = "$.records[*]"
    next_page_token_jsonpath = "$.marker"
    parent_stream_type = ListsStream

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        assert context is not None
        return {
            "list_id": context["list_id"],
            "marker": next_page_token,
        }

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """As needed, append or transform raw data to match expected structure."""
        assert context is not None
        return {**row, "list_id": context["list_id"]}
