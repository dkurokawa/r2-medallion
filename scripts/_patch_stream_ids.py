#!/usr/bin/env python3
"""`wrangler pipelines streams list` の出力を標準入力で受け、wrangler.toml の
stream ID を書き戻す。rebuild-tables.sh の 4/5 で使う。

別ファイルにしてあるのは、スクリプト本体をヒアドキュメントで渡すと標準入力が
そちらに奪われ、パイプで渡した list の出力が読めないため（2026-09-23 に実際に
踏んだ）。
"""
import pathlib
import re
import sys

NEED = ["ppn_datalake_silver_api_metrics", "ppn_datalake_gold_api_metrics_daily"]
ROW = re.compile(r"│\s*(ppn_datalake_\w+)\s*│\s*([0-9a-f]{32})\s*│")


def main() -> None:
    ids = {}
    for line in sys.stdin:
        m = ROW.search(line)
        if m:
            ids[m.group(1)] = m.group(2)

    missing = [n for n in NEED if n not in ids]
    if missing:
        sys.exit(f"!! stream の ID を取れませんでした: {missing}")

    # wrangler.toml の各 stream 行は `stream = "<id>"  # <stream 名>` の形。
    # 名前のコメントが目印なので、無ければ書き換えずに止める（黙って壊さない）。
    p = pathlib.Path("wrangler.toml")
    s = p.read_text(encoding="utf-8")
    for name in NEED:
        pat = re.compile(r'^stream = "[^"]*"\s*#\s*' + re.escape(name) + r"\s*$", re.M)
        if not pat.search(s):
            sys.exit(f"!! wrangler.toml に '# {name}' 付きの stream 行がありません。手で直してください。")
        s = pat.sub(f'stream = "{ids[name]}"  # {name}', s)
    p.write_text(s, encoding="utf-8")
    for name in NEED:
        print(f"  {name} -> {ids[name]}")


if __name__ == "__main__":
    main()
