# Usage and examples

[Docs home](README.md) › Usage and examples

Recipes for each part of the SDK. Runnable scripts with the same flows are in [`talkingdb_sdk/example.py`](../talkingdb_sdk/example.py), [`example_async.py`](../talkingdb_sdk/example_async.py) and [`example_namespaces.py`](../talkingdb_sdk/example_namespaces.py).

```python
import os
from talkingdb_sdk import TalkingDBClient

client = TalkingDBClient(
    host="http://localhost:8090",
    api_key=os.environ["TDB_API_KEY"],
    timeout=30,
)
```

## Upload one document and wait

```python
status = client.ingest_document(
    "annual-report.pdf",
    title="Annual report 2025",
    on_progress=lambda s: print(s.state.value, s.stage.value if s.stage else "-", s.percent),
)
print(status.result_graph_id, status.page_count)
```

## Upload without waiting, then poll

```python
from talkingdb_sdk import JobState

accepted = client.submit_document("annual-report.pdf")
print(accepted.job_id, accepted.state)          # returns at once

final = client.wait_for_terminal(accepted.job_id, poll_interval=2.0, timeout=600)
if final.state == JobState.COMPLETED:
    print("graph:", final.result_graph_id)
elif final.state == JobState.FAILED:
    print("failed:", final.error_code, final.error_message)
```

`wait_for_terminal` raises `TimeoutError` if `timeout` seconds pass first. Cancel a job with `client.cancel_job(job_id)`.

## Upload many documents

```python
statuses = client.ingest_many(["a.pdf", "b.docx", "c.pdf"], namespace="handbooks")
for s in statuses:
    print(s.job_id, s.state.value)
```

When the server's queue is full, `submit_many` and `ingest_many` wait for the server's `Retry-After` and try again. Pass `retry_on_full=False` to fail fast instead.

## Search

```python
elements = client.match_node(
    graph_ids=[status.result_graph_id],
    query="What are the main risks?",
    max_results=5,
)
```

Search several documents at once by passing several graph IDs. The element fields are described in [module-ttt's retrieval guide](https://github.com/TalkingDB/module-ttt/blob/main/docs/retrieval-and-demonstrations.md).

## Organise documents

```python
docs = client.list_documents(limit=50)
projects = client.list_projects()
client.add_document_to_project(projects[0].project_id, job_id=accepted.job_id)
client.rename_project(projects[0].project_id, "Board papers")
client.remove_document(accepted.job_id)          # delete a document and its job
```

## Browse public demo content without a key

```python
anon = TalkingDBClient("http://localhost:8090")
for ns in anon.list_public_namespaces():
    print(ns.namespace, ns.title)
    for doc in anon.list_public_namespace_documents(ns.namespace):
        print("  ", doc.title, doc.suggested_queries)
```

## Handle errors

```python
from talkingdb_sdk import FileTooLargeError, UnsupportedTypeError, UnauthorizedError, JobFailedError

try:
    client.ingest_document("scan.tiff")
except UnsupportedTypeError:
    print("Only PDF and DOCX are accepted")
except FileTooLargeError as e:
    print("Too large:", e.body)
except UnauthorizedError:
    print("Check TDB_API_KEY")
except JobFailedError as e:
    print("Indexing failed:", e.error_code)
```

The full mapping from HTTP status to exception is in [architecture.md](architecture.md#errors).

---

**Related:** [Architecture](architecture.md) · [Troubleshooting](troubleshooting.md) · [README](../README.md)
