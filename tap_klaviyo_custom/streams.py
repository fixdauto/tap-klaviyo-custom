"""Stream type classes for tap-klaviyo-custom."""

import base64
import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast
import copy
import urllib
import json

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
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "lists.json"

    url_base = "https://a.klaviyo.com/api/v2/"

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    api_secret = ''

    def get_url_params(self, partition: Optional[dict]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        params.update({"api_key": self.api_secret})
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

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.
        """
        next_page_token: Any = None
        finished = False
        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = self._request_with_backoff(prepared_request, context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token


    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        for row in self.request_records(context):
            row = self.post_process(row, context)
            yield row



class ListMembersStream(RESTStream):
    """Define custom stream."""
    name = "list_members"
    path = "group/{list_id}/members/all"
    primary_keys = []
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "list_members.json"

    url_base = "https://a.klaviyo.com/api/v2/"

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    api_secret = ''

    def get_url_params(self, partition: Optional[dict]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        params.update({"api_key": self.api_secret})
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
        return request

    def get_url(self, context: Optional[dict], list_id: Optional[str]) -> str:
        """Return a URL, optionally targeted to a specific partition or context.

        Developers override this method to perform dynamic URL generation.
        """
        url = self.url_base+self.path.format(list_id=list_id)

        return url

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.
        """
        next_page_token: Any = None
        finished = False
        list_ids = ['RduZTr']
        for id in list_ids:
            while not finished:
                prepared_request = self.prepare_request(
                    context, next_page_token=next_page_token, list_id=id
                )
                resp = self._request_with_backoff(prepared_request, context)
                resp_json = resp.json()
                result = resp_json['records']
                for row in result:
                    row['list_id'] = id
                    yield row
                
                #pulls marker from json response to use in next page API call
                #breaks the loop when no marker is returned in the response
                if 'marker' in resp_json.keys():
                    next_page_token = resp_json['marker']
                else:
                    finished = True


    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.
        """
        for row in self.request_records(context):
            row = self.post_process(row, context)
            yield row