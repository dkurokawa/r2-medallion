"""Remove duplicate rows from silver / gold by rewriting a single day.

A retried Workflow step can re-send rows that Pipelines already accepted
(at-least-once delivery), so the same row occasionally lands twice (see README
"Design decisions §6"). R2 SQL is read-only and cannot delete, so this script
rewrites the Iceberg table directly with PyIceberg.

It keeps one row per `row_uid` (= source R2 object key + line number) and replaces
that day's rows wholesale. If anything goes wrong, bronze is untouched, so the day
can be rebuilt with `POST /run?dt=<dt>&attempt=<n>&force=1`.

**Targeting one day still rewrites other days' files.** The table's data files are
split by ingest time (`__ingest_ts`), not by `dt`; the 240-day backfill sits in a
single file, so `dedupe` rewrites that whole file. Row contents should not change,
but **always run `verify-all` after `dedupe`.**

Setup (once; creates a venv inside the repo, which is git-ignored):
    python3 -m venv .venv
    .venv/bin/pip install "pyiceberg[pyarrow,pyiceberg-core]"

Usage:
    .venv/bin/python scripts/dedupe_day.py scan-all [silver|gold]      # find days with duplicates
    .venv/bin/python scripts/dedupe_day.py count <dt> [silver|gold]    # count one day
    .venv/bin/python scripts/dedupe_day.py dedupe <dt> [silver|gold]   # actually remove them
    .venv/bin/python scripts/dedupe_day.py verify-all                  # check every day of both layers against its manifest

The layer defaults to silver. gold duplicates through the same `row_uid` mechanism
(re-running a day re-inserts the same rows into both silver and gold).

Environment:
    CF_ACCOUNT_ID           Cloudflare account id
    WORKER_URL              the deployed Worker's URL (for `verify-all`)
    R2_CATALOG_TOKEN_FILE   file holding an API token with R2 Data Catalog + R2 Storage
                            permissions (default ~/.config/r2-medallion/r2-catalog-token)
    ADMIN_TOKEN_FILE        file holding the Worker's ADMIN_TOKEN, used by `verify-all`
                            to fetch manifests via GET /status
                            (default ~/.config/r2-medallion/admin-token)
Tokens are read from files and never printed.
"""

import collections
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request

from pyiceberg.catalog.rest import RestCatalog

ACCOUNT_ID = os.environ["CF_ACCOUNT_ID"]
BUCKET = "ppn-datalake-bronze"
TOKEN_PATH = pathlib.Path(os.environ.get("R2_CATALOG_TOKEN_FILE", pathlib.Path.home() / ".config/r2-medallion/r2-catalog-token"))
ADMIN_TOKEN_PATH = pathlib.Path(os.environ.get("ADMIN_TOKEN_FILE", pathlib.Path.home() / ".config/r2-medallion/admin-token"))
WORKER_URL = os.environ["WORKER_URL"]

# layer → (namespace, table, manifest key holding that layer's row count)
LAYERS = {
    "silver": ("silver", "api_metrics", "silverRows"),
    "gold": ("gold", "api_metrics_daily", "goldRows"),
}


def load_table(layer: str = "silver"):
    namespace, name, _ = LAYERS[layer]
    token = TOKEN_PATH.read_text().strip()
    catalog = RestCatalog(
        name="r2",
        warehouse=f"{ACCOUNT_ID}_{BUCKET}",
        uri=f"https://catalog.cloudflarestorage.com/{ACCOUNT_ID}/{BUCKET}",
        token=token,
    )
    return catalog.load_table((namespace, name))


def day_counts(table, dt: str) -> tuple[int, int]:
    arrow = table.scan(row_filter=f"dt == '{dt}'", selected_fields=("row_uid",)).to_arrow()
    return arrow.num_rows, len(set(arrow.column("row_uid").to_pylist()))


def per_day_counts(table) -> dict[str, tuple[int, int]]:
    arrow = table.scan(selected_fields=("dt", "row_uid")).to_arrow()
    per = collections.defaultdict(lambda: [0, set()])
    for dt, uid in zip(arrow.column("dt").to_pylist(), arrow.column("row_uid").to_pylist()):
        per[dt][0] += 1
        per[dt][1].add(uid)
    return {dt: (rows, len(uids)) for dt, (rows, uids) in per.items()}


def fetch_manifest(dt: str, admin_token: str) -> dict | None:
    req = urllib.request.Request(
        f"{WORKER_URL}/status?dt={dt}",
        # The default User-Agent (Python-urllib) gets a 403 from Cloudflare.
        headers={"Authorization": f"Bearer {admin_token}", "User-Agent": "r2-medallion-dedupe"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            return json.loads(res.read())["manifest"]
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return None
        raise


def cmd_count(dt: str, layer: str) -> None:
    rows, distinct = day_counts(load_table(layer), dt)
    print(f"{dt}: rows={rows} distinct={distinct} dup={rows - distinct}")


def cmd_scan_all(layer: str) -> None:
    per = per_day_counts(load_table(layer))
    bad = {dt: c for dt, c in per.items() if c[0] != c[1]}
    print(f"days={len(per)} rows={sum(c[0] for c in per.values())} dup_days={len(bad)}")
    for dt, (rows, distinct) in sorted(bad.items()):
        print(f"  {dt}: rows={rows} distinct={distinct} dup={rows - distinct}")


def cmd_verify_all() -> None:
    """Compare each day's row counts in both layers with that day's manifest (what the Workflow meant to send)."""
    per = {layer: per_day_counts(load_table(layer)) for layer in LAYERS}
    admin_token = ADMIN_TOKEN_PATH.read_text().strip()
    bad, no_manifest = [], []
    for dt in sorted(set().union(*per.values())):
        manifest = fetch_manifest(dt, admin_token)
        if manifest is None:
            no_manifest.append(dt)
            continue
        for layer, (_, _, key) in LAYERS.items():
            rows, distinct = per[layer].get(dt, (0, 0))
            want = manifest[key]
            if rows != want or distinct != want:
                bad.append((layer, dt, rows, distinct, want))
    for layer, counts in per.items():
        print(f"{layer}: days={len(counts)} rows={sum(c[0] for c in counts.values())}")
    print(f"mismatches against manifest: {len(bad)}")
    for layer, dt, rows, distinct, want in bad:
        print(f"  {layer} {dt}: table rows={rows} distinct={distinct} / manifest={want}")
    if no_manifest:
        print(f"days without a manifest: {no_manifest}")
    if bad or no_manifest:
        sys.exit(1)


def cmd_dedupe(dt: str, layer: str) -> None:
    table = load_table(layer)
    before_rows, before_distinct = day_counts(table, dt)
    print(f"before: rows={before_rows} distinct={before_distinct} dup={before_rows - before_distinct}")
    if before_rows == before_distinct:
        print("no duplicates; nothing to do.")
        return

    full = table.scan(row_filter=f"dt == '{dt}'").to_arrow()
    # Duplicate rows are byte-identical, so keeping the first row per row_uid is enough.
    seen: set[str] = set()
    keep: list[int] = []
    for i, uid in enumerate(full.column("row_uid").to_pylist()):
        if uid in seen:
            continue
        seen.add(uid)
        keep.append(i)
    deduped = full.take(keep)
    print(f"rows to write back: {deduped.num_rows}")

    # Replace only this day's rows (the file is rewritten with other days in it; see the docstring).
    table.overwrite(deduped, overwrite_filter=f"dt == '{dt}'")

    after_rows, after_distinct = day_counts(load_table(layer), dt)
    print(f"after: rows={after_rows} distinct={after_distinct} dup={after_rows - after_distinct}")
    if after_rows != before_distinct:
        sys.exit(f"!! expected {before_distinct} rows, got {after_rows}. Rebuild this day.")


def main() -> None:
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    cmd, rest = args[0], args[1:]
    layer = "silver"
    if rest and rest[-1] in LAYERS:
        layer = rest.pop()
    if cmd == "scan-all" and not rest:
        cmd_scan_all(layer)
    elif cmd == "count" and len(rest) == 1:
        cmd_count(rest[0], layer)
    elif cmd == "dedupe" and len(rest) == 1:
        cmd_dedupe(rest[0], layer)
    elif cmd == "verify-all" and not rest:
        cmd_verify_all()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
