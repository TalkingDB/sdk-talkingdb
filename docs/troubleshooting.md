# Troubleshooting

[Docs home](README.md) › Troubleshooting

| Symptom | Likely cause | Fix |
| :--- | :--- | :--- |
| `requests.exceptions.ConnectionError` after several seconds | No TTT service at `host`, or a wrong port | Start [module-ttt](https://github.com/TalkingDB/module-ttt#run-it-in-four-steps) and check `GET {host}/health` |
| `UnauthorizedError` (401) | Missing or wrong API key | Create one with `POST /auth/api-keys` on the TTT service and pass it as `api_key=` |
| `UnsupportedTypeError` (415) | The file is not `.pdf` or `.docx` | Convert it first |
| `FileTooLargeError` (413) | Over the server's limit (PDF 100 MB, DOCX 50 MB by default) | Split the file, or ask the operator to raise the server limit |
| `QueueFullError` (429) from `submit_document` | The server's ingestion queue is full | Wait `retry_after_seconds`, or use `submit_many`, which waits and retries for you |
| `SpoolExhaustedError` (503) | The server is short of temporary disk space | Retry after `retry_after_seconds`; tell the operator if it persists |
| `JobFailedError` with `PASSWORD_PROTECTED` | The PDF is encrypted | Remove the password |
| `JobFailedError` with `PROCESSING_FAILED` on a scanned PDF | The PDF has no text layer; [OCR](https://en.wikipedia.org/wiki/Optical_character_recognition) is not supported | Run OCR first |
| `TimeoutError` from `wait_for_terminal` | The job is still running when `timeout` ran out | Raise `timeout`, or keep the `job_id` and poll later |
| `match_node` returns `[]` | Wrong `graph_ids`, or nothing in the document matches the words | Use `result_graph_id` from a `COMPLETED` job; rephrase with words from the document |
| `ModuleNotFoundError: No module named 'talkingdb_sdk'` | Installed into a different Python | Install with the same interpreter: `python -m pip install "git+https://github.com/TalkingDB/sdk-talkingdb.git"` |

---

**Related:** [Architecture](architecture.md) · [Usage and examples](usage-and-examples.md) · [README](../README.md)
