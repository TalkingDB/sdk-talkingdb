import threading
from typing import List, Optional

import requests
from requests.exceptions import RequestException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception,
)


def _is_retryable_exception(exc: Exception) -> bool:
    """
    Retry on:
    - Network errors
    - Timeouts
    - 5xx HTTP errors

    Do NOT retry on 4xx.
    """
    if isinstance(exc, RequestException):
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            return 500 <= exc.response.status_code < 600
        return True  # connection errors, timeouts, etc.
    return False


class TalkingDBClient:
    def __init__(self, host: str, timeout: float = 30.0):
        self.host = host.rstrip("/")
        self.timeout = timeout
        self._local = threading.local()

    def _get_session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            session = requests.Session()
            session.headers.update({
                "Content-Type": "application/json",
            })
            self._local.session = session
        return self._local.session

    @retry(
        retry=retry_if_exception(_is_retryable_exception),
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=10),
        reraise=True,
    )
    def _post(self, url: str, payload: dict) -> requests.Response:
        res = self._get_session().post(
            url,
            json=payload,
            timeout=self.timeout,
        )
        res.raise_for_status()
        return res

    def index_document(
        self,
        document: dict,
        file_index: dict,
        metadata: dict,
    ) -> Optional[str]:
        url = f"{self.host}/index/document/elements"
        payload = {
            "metadata": metadata,
            "document": document,
            "file_index": file_index,
        }

        res = self._post(url, payload)
        return res.json().get("graph_id")

    def match_node(
        self,
        graph_ids: List[str],
        query: str,
        metadata: dict | None = None,
    ) -> list:
        url = f"{self.host}/extract"
        nodes = []

        for graph_id in graph_ids:
            payload = {
                "graph_id": graph_id,
                "text": query,
            }
            if metadata is not None:
                payload["metadata"] = metadata

            res = self._post(url, payload)
            nodes.extend(res.json().get("elements", []))

        return nodes
