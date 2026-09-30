#!/usr/bin/env python3
"""Read `wrangler pipelines streams list` output on stdin and write the stream
ids back into wrangler.toml. Used by step 4/5 of rebuild-tables.sh.

This is a separate file because passing the script as a heredoc would consume
stdin, leaving the piped `list` output unreadable (hit on 2026-09-23).
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
        sys.exit(f"!! could not find stream ids for: {missing}")

    # Each stream line in wrangler.toml looks like `stream = "<id>"  # <stream name>`.
    # The name comment is the anchor; if it is missing, stop instead of silently breaking the file.
    p = pathlib.Path("wrangler.toml")
    s = p.read_text(encoding="utf-8")
    for name in NEED:
        pat = re.compile(r'^stream = "[^"]*"\s*#\s*' + re.escape(name) + r"\s*$", re.M)
        if not pat.search(s):
            sys.exit(f"!! wrangler.toml has no stream line tagged '# {name}'. Fix it by hand.")
        s = pat.sub(f'stream = "{ids[name]}"  # {name}', s)
    p.write_text(s, encoding="utf-8")
    for name in NEED:
        print(f"  {name} -> {ids[name]}")


if __name__ == "__main__":
    main()
