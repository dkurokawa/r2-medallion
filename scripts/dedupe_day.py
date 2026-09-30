"""silver / gold に入った重複行を、その日だけ書き換えて実際に消す。

Pipelines の `send()` は、サーバーが受け取っていてもエラーを返すことがある
（at-least-once）。こちらは「送れなかった」と判断して再送するので、まれに同じ
行が 2 度入る（240 日の取り込みで 1 日ぶん）。R2 SQL は読み取り専用で消せない
ので、Iceberg のテーブルを PyIceberg から直接書き換える。

`row_uid`（= 元の R2 オブジェクトのキー + 行番号）ごとに 1 行だけ残し、その日の
行を丸ごと置き換える。失敗しても bronze は無傷なので、
`POST /run?dt=<dt>&attempt=<n>&force=1` でその日を作り直せる。

**1 日だけ指定しても、他の日のファイルまで書き換わる。** テーブルの実ファイルは
取り込み日（`__ingest_ts`）ごとに分かれていて `dt` では分かれていない。backfill の
240 日ぶんは 1 ファイルに入っているので、`dedupe` はそのファイルごと書き直す。
行の中身は変わらないはずだが、**`dedupe` の後は必ず `verify-all` を実行する。**

準備（初回のみ。リポジトリ外に venv を作る）:
    python3 -m venv ~/.venvs/iceberg
    ~/.venvs/iceberg/bin/pip install "pyiceberg[pyarrow,pyiceberg-core]"

使い方:
    ~/.venvs/iceberg/bin/python scripts/dedupe_day.py scan-all [silver|gold]      # 重複のある日を探す
    ~/.venvs/iceberg/bin/python scripts/dedupe_day.py count <dt> [silver|gold]    # その日を数える
    ~/.venvs/iceberg/bin/python scripts/dedupe_day.py dedupe <dt> [silver|gold]   # 実際に消す
    ~/.venvs/iceberg/bin/python scripts/dedupe_day.py verify-all                  # 両層の全日を manifest と照合

層を省くと silver。gold も同じ `row_uid` の仕組みで重複する（1 日を流し直すと
silver・gold の両方に同じ行がもう一度入る）。

トークンは `$R2_CATALOG_TOKEN_FILE`（既定 `~/.config/r2-medallion/r2-catalog-token`）（R2 Data Catalog + R2 Storage の権限）
から読む。`verify-all` は Worker の `GET /status` で manifest を取るので、
`$ADMIN_TOKEN_FILE`（既定 `~/.config/r2-medallion/admin-token`、Worker の ADMIN_TOKEN）も使う。
どちらも画面には出さない。
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

# 層 → (namespace, table, その層の行数を持つ manifest のキー)
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
        # 既定の User-Agent（Python-urllib）は Cloudflare に 403 で弾かれる。
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
    """両層の日ごとの行数を、その日の manifest（Workflow が送ったつもりの行数）と突き合わせる。"""
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
    print(f"manifest との不一致: {len(bad)} 件")
    for layer, dt, rows, distinct, want in bad:
        print(f"  {layer} {dt}: table rows={rows} distinct={distinct} / manifest={want}")
    if no_manifest:
        print(f"manifest が無い日: {no_manifest}")
    if bad or no_manifest:
        sys.exit(1)


def cmd_dedupe(dt: str, layer: str) -> None:
    table = load_table(layer)
    before_rows, before_distinct = day_counts(table, dt)
    print(f"before: rows={before_rows} distinct={before_distinct} dup={before_rows - before_distinct}")
    if before_rows == before_distinct:
        print("重複なし。何もしない。")
        return

    full = table.scan(row_filter=f"dt == '{dt}'").to_arrow()
    # 重複行は中身がまったく同じなので、row_uid ごとに最初の1行を残せばよい。
    seen: set[str] = set()
    keep: list[int] = []
    for i, uid in enumerate(full.column("row_uid").to_pylist()):
        if uid in seen:
            continue
        seen.add(uid)
        keep.append(i)
    deduped = full.take(keep)
    print(f"書き戻す行数: {deduped.num_rows}")

    # その日の行だけを置き換える（ファイルは他の日ごと書き直される。上の docstring 参照）。
    table.overwrite(deduped, overwrite_filter=f"dt == '{dt}'")

    after_rows, after_distinct = day_counts(load_table(layer), dt)
    print(f"after: rows={after_rows} distinct={after_distinct} dup={after_rows - after_distinct}")
    if after_rows != before_distinct:
        sys.exit(f"!! 期待した行数 {before_distinct} と違う（{after_rows}）。その日を作り直すこと。")


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
