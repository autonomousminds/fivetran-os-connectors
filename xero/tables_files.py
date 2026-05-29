"""
Sync logic for Xero Files API.

The Files API doesn't support `If-Modified-Since` (we tested — the header
is silently ignored) and there is no batch endpoint for associations
(`/Files/Associations`, `/Associations`, `?AssociationsIncluded=true` all
404 or return summary data without assocs). So we pay 1 GET per file's
`/Files/{id}/Associations`, and for orgs with ~1000 files that alone is
~1000 calls — close to the entire 5000-call daily budget.

Two-mode cursor handles this:

  Backfill mode (state["files_backfill_complete"] != "true"):
    - Sort ASC by UpdatedDateUtc (oldest first)
    - Skip files whose UpdatedDateUtc <= cursor (already done in a prior run)
    - Process each remaining file: upsert + N associations
    - Checkpoint state AFTER EACH PAGE so a daily-rate-limit interrupt
      doesn't lose progress (the previous design only saved the cursor at
      end-of-function, so an interrupted sync left cursor=None and the
      backfill restarted from page 1 every day — never converging).
    - When pagination runs out (page returns <100 items), mark backfill
      complete and switch to steady-state from the next run on.

  Steady-state mode (state["files_backfill_complete"] == "true"):
    - Sort DESC by UpdatedDateUtc (newest first)
    - Short-circuit at the first file with UpdatedDateUtc <= cursor
    - Typically reads 1 page and stops, so daily cost is tiny.

Folders are tiny (~5 rows) so we full-resync them every time.

Limitations:
  - If an association is *removed* on a file that hasn't been modified, we
    won't notice. Stale assoc rows will leak. Acceptable; revisit if needed.
  - File deletions are not tracked. Same caveat.
"""

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from api_client import FILES_BASE, api_request
from helpers import upsert as _upsert


def _list_folders(config):
    url = f"{FILES_BASE}/Folders"
    data = api_request(config, url, scope_group="files")
    return data if isinstance(data, list) else data.get("Items", []) or []


def _list_associations(config, file_id):
    url = f"{FILES_BASE}/Files/{file_id}/Associations"
    data = api_request(config, url, scope_group="files")
    return data if isinstance(data, list) else data.get("Items", []) or []


def sync_files(config, state):
    """Two-mode file + association sync.

    Backfill mode walks oldest→newest with incremental checkpointing so a
    daily-rate-limit interrupt preserves progress. Steady-state mode walks
    newest→oldest and short-circuits at the cursor — the cheap path.
    """
    cursor_key = "files_max_updated_utc"
    backfill_done = state.get("files_backfill_complete") == "true"
    cursor = state.get(cursor_key) or ""

    max_seen = cursor
    files_processed = 0
    associations_processed = 0
    stopped_early = False

    # ASC during backfill (so cursor advances monotonically and partial
    # progress is preserved across daily-limit interrupts); DESC after, to
    # keep steady-state syncs to a single page in the common case.
    direction = "desc" if backfill_done else "asc"

    page = 1
    while True:
        url = f"{FILES_BASE}/Files"
        data = api_request(config, url,
                           params={
                               "page": page,
                               "pagesize": 100,
                               "sort": "UpdatedDateUtc",
                               "direction": direction,
                           },
                           scope_group="files")
        items = data.get("Items", []) or []
        if not items:
            break

        for f in items:
            file_id = f.get("Id") or f.get("FileId") or ""
            if not file_id:
                continue
            updated = f.get("UpdatedDateUtc", "") or ""

            # Cursor handling differs by mode. DESC + cursor → short-circuit
            # because everything past this point is older (already synced).
            # ASC + cursor → skip and continue, because newer files (which
            # we haven't seen yet) are still ahead on later pages.
            if cursor and updated and updated <= cursor:
                if backfill_done:
                    stopped_early = True
                    break
                else:
                    continue

            if updated and updated > max_seen:
                max_seen = updated

            _upsert("files_file", {
                "FileId":         file_id,
                "FolderId":       f.get("FolderId", ""),
                "Name":           f.get("Name", ""),
                "MimeType":       f.get("MimeType", ""),
                "Size":           f.get("Size"),
                "User":           (f.get("User") or {}).get("Name", ""),
                "CreatedDateUtc": f.get("CreatedDateUtc", ""),
                "UpdatedDateUtc": updated,
            })
            files_processed += 1

            for assoc in _list_associations(config, file_id):
                _upsert("files_association", {
                    "FileId":      file_id,
                    "ObjectId":    assoc.get("ObjectId", ""),
                    "ObjectType":  assoc.get("ObjectType", ""),
                    "ObjectGroup": assoc.get("ObjectGroup", ""),
                })
                associations_processed += 1

        # Checkpoint after each page so partial progress survives a
        # DailyRateLimitExceeded raise from the next page's first call.
        if max_seen:
            state[cursor_key] = max_seen
        op.checkpoint(state)

        if stopped_early or len(items) < 100:
            break
        page += 1

    # Last page returned <100 items — we've walked through every file in
    # the org. Mark backfill done so subsequent runs use the cheap
    # DESC short-circuit path.
    if not backfill_done and not stopped_early:
        state["files_backfill_complete"] = "true"
        op.checkpoint(state)

    if backfill_done and stopped_early and files_processed == 0:
        log.info("Files: no changes since last sync — 1 API call total")
    else:
        mode = "steady-state" if backfill_done else "backfill"
        log.info(
            f"Files ({mode}): {files_processed} files, "
            f"{associations_processed} associations synced (cursor → {max_seen})"
        )


def sync_folders(config, state):
    """Sync Folders (lightweight, single call, full sync each run)."""
    count = 0
    for folder in _list_folders(config):
        _upsert("files_folder", {
            "FolderId":  folder.get("Id") or folder.get("FolderId", ""),
            "Name":      folder.get("Name", ""),
            "FileCount": folder.get("FileCount"),
            "Email":     folder.get("Email", ""),
            "IsInbox":   folder.get("IsInbox", False),
        })
        count += 1
    log.info(f"Folders: {count} synced")


FILES_SYNCS = [sync_folders, sync_files]
