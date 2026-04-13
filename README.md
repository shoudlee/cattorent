# CatTorrent

A lightweight peer-to-peer file sharing and downloading prototype in Python.

## What this project does

- Discover online peers via UDP broadcast (`ONLI`).
- Exchange file list via TCP control channel (`LIST` -> `RLST`).
- Fetch metadata (`META` -> `RMTA`).
- Download file pieces over TCP data channel (`GETP` -> `PIEC`).
- Support concurrent piece download with multiple candidate peers.

## Repository structure

- `cmd.py`: CLI entry.
- `protocol.py`: protocol orchestration, peer discovery, download startup.
- `connections.py`: connection lifecycle and handler registry.
- `connection_handler.py`: control/data socket send-recv workers.
- `download_manager.py`: per-download task queue and worker management.
- `download_worker.py`: piece fetch/retry/write loop.
- `protocol_codec.py`: frame encoding/decoding.
- `tcp_utils.py`: socket utils and event waiter.
- `catshare/`: shared files and downloaded metadata.
- `protocol/`: protocol specs.

## Concurrent download flow

Current behavior is:

1. The input pair (`peer_id`, `filename`) defines the primary peer.
2. Before downloading starts, the primary peer is used to fetch metadata once.
3. Candidate peers are discovered once before download starts (not per piece).
4. Only peers that report a complete matching file (same name and file size) are selected as candidates.
5. One `DownloadManager` creates one `DownloadWorker` per selected data connection.
6. All workers consume the same piece queue concurrently.

## Requirements

- Python 3.10+ recommended.
- Local network access for peer discovery and TCP connections.

## Quick start

1. Open terminal in project root.
2. Run:

```bash
python cmd.py
```

By default, client goes online when started.

## CLI commands

- `online` : start protocol workers (if not already online).
- `peer` : print current discovered peers.
- `list <peer_id>` : request file list from a peer.
- `meta <filename>` : generate local `.filename.meta` from local shared file.
- `meta <peer_id> <filename>` : request metadata from a peer.
- `file <peer_id> <dst_path> <filename>` : start file download.
- `exit` : stop workers and exit.

## Notes

- Shared files are served from `catshare/`.
- Metadata is stored as `.filename.meta` in `catshare/`.
- Download destination path can be absolute or relative.
- For best concurrency effect, keep multiple peers online with the same full file.

## Protocol reference

See:

- `protocol/cattorrent_v0.1.md`
- `protocol/cattorrent_v1.0.md`
