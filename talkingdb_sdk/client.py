from typing import List, Optional
import requests


class TalkingDBClient:
    def __init__(self, host: str):
        self.host = host
        self.session = requests.Session()

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
        res = self.session.post(url, json=payload)
        res.raise_for_status()
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

            res = self.session.post(url, json=payload)
            res.raise_for_status()
            nodes.extend(res.json().get("elements", []))

        return nodes
