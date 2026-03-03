"""Marketo target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import requests

from hotglue_singer_sdk.exceptions import RetriableAPIError
from hotglue_singer_sdk.target_sdk.client import HotglueSink


class MarketoSink(HotglueSink):
    """Marketo target sink class."""

    base_url = "https://trackcmp.net"

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}, verify=True
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers = self.http_headers

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            data=request_data,
        )
        self.validate_response(response)
        return response
