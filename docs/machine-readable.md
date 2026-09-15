# Machine-readable output

`ocopy --machine-readable` emits a **JSON Lines** (JSONL, also called NDJSON) event stream on `stdout` instead of the human progress bar. Each line is one JSON object with a `type` field. Pipe it into `jq`, parse it line-by-line in a script, or feed it into a structured log pipeline.

```sh
ocopy --machine-readable /path/to/src /path/to/dst | jq -c .
```

Exit codes are unchanged:

| Code | Meaning |
| ---- | ------- |
| 0    | Success (final event: `result/ok`) |
| 1    | Error (final event: `result/error`) |
| 3    | Cancelled by user (final event: `result/cancelled`) |

`stderr` is reserved for unhandled exceptions; nothing this contract promises is written there.

## Schema versioning

The `start` event carries `schema_version` (currently **1**). The version is bumped on any incompatible change to the contract. Field additions to existing event types are not breaking — consumers should ignore unknown fields.

## Stream invariants

- One JSON object per line. Lines never wrap.
- The **first** event is always `start`. Exception: a pre-`start` failure (today: insufficient disk space) emits one `result` event and nothing else.
- The **last** event is always `result`. Consumers should read until `type == "result"` and dispatch on `status`.
- Exactly one `result` event per run.
- `warning` and `progress` events appear between `start` and `result` in any quantity (including zero).
- Every event has at least `type` (string) and `ts` (float, seconds since epoch).

## Event reference

### `start`

Emitted once, immediately after pre-flight checks pass.

| Field | Type | Meaning |
| --- | --- | --- |
| `type` | `"start"` | |
| `ts` | float | Unix epoch seconds. |
| `schema_version` | int | Currently `1`. |
| `source` | string | Absolute source path. |
| `destinations` | string[] | Absolute destination paths in argument order. |
| `hash_algorithm` | string | e.g. `"xxh64"`. |
| `verify` | bool | Re-read and verify after copy. |
| `skip_existing` | bool | Skip files whose size+mtime+trusted hash match. |
| `mhl` | bool | Write ASC MHL output. |
| `legacy_mhl` | bool | Use legacy flat `.mhl` instead of ASC MHL. |
| `contents` | bool | `--contents` was given: files land directly in each destination instead of `destination/<source name>`. |
| `total_bytes` | int | Total source size to copy. |

### `warning`

Emitted zero or more times between `start` and `result`. Warnings never change the final status.

| Field | Type | Meaning |
| --- | --- | --- |
| `type` | `"warning"` | |
| `ts` | float | Unix epoch seconds. |
| `code` | string | One of the codes below. |
| `message` | string | Human-readable explanation. |
| _extras_ | any | Some codes attach extra fields (e.g. `destination`, `paths`, `command`). |

Codes:

| Code | When | Extras |
| --- | --- | --- |
| `same_drive` | Two or more destinations resolve to the same mount. | — |
| `sleep_inhibit_unavailable` | OS sleep inhibitor could not be acquired. | — |
| `missing_files` | A destination is missing files after copy (should never happen — please report). | `destination` (string), `paths` (string[]) |
| `in_progress_leftover` | A destination still has `*.copy_in_progress` files after copy (should never happen — please report). | `destination` (string), `paths` (string[]) |
| `update_available` | A newer ocopy release is on PyPI. | `command` (string, the suggested upgrade command) |

### `progress`

Emitted on each percent tick (≈100 events for a complete run, fewer if the run is short).

| Field | Type | Meaning |
| --- | --- | --- |
| `type` | `"progress"` | |
| `ts` | float | Unix epoch seconds. |
| `percent` | int | 0–100, monotonically non-decreasing. |
| `phase` | string | `"copy"` or `"verify"`. Verify only appears when `--verify` is on. |
| `current_file` | string \| null | Basename of the file currently being processed. `null` until the first chunk lands. |
| `speed_bytes_per_sec` | float | Windowed live throughput at emit time (bytes/second). Computed over the trailing ~5 seconds of the **current phase**; falls back to cumulative until enough samples accumulate. **Differs** from the same-named field on `result`, which is the cumulative run average. |
| `eta_seconds` | float | Estimated remaining time in seconds. Projects remaining copy work at the current copy rate and remaining verify work at the current verify rate; before verify samples exist, verify is projected at the copy rate (conservative). `0` once the run is finished. |

### `result`

The terminal event. Exactly one per run.

`status == "ok"`:

| Field | Type | Meaning |
| --- | --- | --- |
| `type` | `"result"` | |
| `status` | `"ok"` | |
| `ts` | float | Unix epoch seconds. |
| `files_copied` | int | Files actually copied (not skipped). |
| `files_skipped` | int | Files skipped due to `--skip-existing`. |
| `bytes_copied` | int | Same as `start.total_bytes`. |
| `duration_s` | float | Wall-clock duration of the run. |
| `speed_bytes_per_sec` | float | Average throughput across the run. |
| `destinations` | string[] | Absolute destination paths. |
| `hash_algorithm` | string | Echoes `start.hash_algorithm`. |

`status == "cancelled"` (exit 3):

| Field | Type | Meaning |
| --- | --- | --- |
| `type` | `"result"` | |
| `status` | `"cancelled"` | |
| `ts` | float | Unix epoch seconds. |
| `files_verified` | int | Files verified before cancellation. |
| `checkpoints` | string[] | Absolute paths to `.ocopy-checkpoint` files. Re-run `ocopy` to resume. |

`status == "error"` (exit 1):

| Field | Type | Meaning |
| --- | --- | --- |
| `type` | `"result"` | |
| `status` | `"error"` | |
| `ts` | float | Unix epoch seconds. |
| `code` | string | `insufficient_space` \| `copy_failed`. |
| `errors` | object[] | Per-failure detail. For `copy_failed`: `{source, destinations[], message}`. For `insufficient_space`: `{destination, needed_bytes, available_bytes, message}`. |
| `files_copied` | int | Files completed before failure (may be 0). |
| `files_skipped` | int | Files skipped before failure. |

## Worked examples

### Successful run

```
{"type":"start","ts":1731350400.12,"schema_version":1,"source":"/Volumes/Card/A001","destinations":["/Volumes/Backup1/A001"],"hash_algorithm":"xxh64","verify":true,"skip_existing":true,"mhl":true,"legacy_mhl":false,"contents":false,"total_bytes":12345678}
{"type":"progress","ts":1731350401.01,"percent":4,"phase":"copy","current_file":"clip_001.mov","speed_bytes_per_sec":5.0e7,"eta_seconds":18.4}
{"type":"progress","ts":1731350405.40,"percent":52,"phase":"copy","current_file":"clip_004.mov","speed_bytes_per_sec":6.1e7,"eta_seconds":9.7}
{"type":"progress","ts":1731350410.55,"percent":98,"phase":"verify","current_file":"clip_008.mov","speed_bytes_per_sec":1.4e8,"eta_seconds":0.3}
{"type":"result","status":"ok","ts":1731350410.92,"files_copied":8,"files_skipped":0,"bytes_copied":12345678,"duration_s":10.80,"speed_bytes_per_sec":5.7e7,"destinations":["/Volumes/Backup1/A001"],"hash_algorithm":"xxh64"}
```

### Cancellation

```
{"type":"start","ts":1731350400.12,"schema_version":1,"source":"/Volumes/Card/A001","destinations":["/Volumes/Backup1/A001"],"hash_algorithm":"xxh64","verify":true,"skip_existing":true,"mhl":true,"legacy_mhl":false,"contents":false,"total_bytes":12345678}
{"type":"progress","ts":1731350403.01,"percent":24,"phase":"copy","current_file":"clip_002.mov","speed_bytes_per_sec":4.8e7}
{"type":"result","status":"cancelled","ts":1731350403.40,"files_verified":2,"checkpoints":["/Volumes/Backup1/A001/.ocopy-checkpoint"]}
```

### Error

```
{"type":"start","ts":1731350400.12,"schema_version":1,"source":"/Volumes/Card/A001","destinations":["/Volumes/Backup1/A001"],"hash_algorithm":"xxh64","verify":true,"skip_existing":true,"mhl":true,"legacy_mhl":false,"contents":false,"total_bytes":12345678}
{"type":"progress","ts":1731350402.01,"percent":18,"phase":"copy","current_file":"clip_002.mov","speed_bytes_per_sec":4.5e7}
{"type":"result","status":"error","ts":1731350402.50,"code":"copy_failed","errors":[{"source":"/Volumes/Card/A001/clip_002.mov","destinations":["/Volumes/Backup1/A001/clip_002.mov"],"message":"IO Error"}],"files_copied":1,"files_skipped":0}
```

## Consumer recipe (Python)

```python
import json
import subprocess
import sys

proc = subprocess.Popen(
    ["ocopy", "--machine-readable", "/src", "/dst"],
    stdout=subprocess.PIPE,
    text=True,
)
assert proc.stdout is not None
for line in proc.stdout:
    event = json.loads(line)
    if event["type"] == "progress":
        print(f"{event['percent']}% {event['phase']} {event['current_file']}")
    elif event["type"] == "warning":
        print(f"warning [{event['code']}]: {event['message']}", file=sys.stderr)
    elif event["type"] == "result":
        print(f"finished: {event['status']}")
        break
proc.wait()
sys.exit(proc.returncode)
```
