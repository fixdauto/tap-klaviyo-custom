"""Stream type classes for tap-klaviyo-custom."""

import base64
import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast
import copy
import urllib
import json
import time

from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
import tap_klaviyo_custom.tap

import singer

LOGGER = singer.get_logger()

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ListsStream(RESTStream):
    """Define custom stream."""
    name = "lists"
    path = "lists"
    primary_keys = ["list_id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "lists.json"
    #Defining the url_base outside of the class results in an error
    url_base = 'https://a.klaviyo.com/api/v2/'

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    def get_url_params(self, partition: Optional[dict]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        params.update({"api_key": self.config['api_key']})
        return params

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.
        """
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                )
            ),
        )
        return request

    def get_url(self, context: Optional[dict]) -> str:
        """Return a URL, optionally targeted to a specific partition or context.

        Developers override this method to perform dynamic URL generation.
        """
        url = self.url_base+self.path

        return url



class ListMembersStream(RESTStream):
    """Define custom stream."""
    name = "list_members"
    path = "group/{list_id}/members/all"
    primary_keys = ["email"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "list_members.json"
    #Defining the url_base outside of the class results in an error
    url_base = 'https://a.klaviyo.com/api/v2/'

    records_jsonpath = "$.records[*]"

    def get_url_params(self, partition: Optional[dict]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        params.update({"api_key": self.config['api_key']})
        return params

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any], list_id: Optional[str]
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.
        """
        http_method = self.rest_method
        url: str = self.get_url(context, list_id)
        params: dict = self.get_url_params(context)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        if next_page_token != None:
            params['marker'] = next_page_token

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                )
            ),
        )
        # sleep timer to avoid Klaviyo API rate limit errors
        time.sleep(1)
        return request

    def get_url(self, context: Optional[dict], list_id: Optional[str]) -> str:
        """Return a URL, optionally targeted to a specific partition or context.

        Developers override this method to perform dynamic URL generation.
        """
        url = self.url_base+self.path.format(list_id=list_id)

        return url


    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        list_ids = self.config["listIDs"]
        # loops through the Klaviyo list IDs and updates the URL path to include each
        for id in list_ids:
            path = f"group/{id}/members/all"
            for row in self.request_records(context):
                row['list_id'] = id
                row = self.post_process(row, context)
                yield row