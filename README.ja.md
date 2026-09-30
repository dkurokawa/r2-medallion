# r2-medallion（日本語版）

R2 に置かれたリクエスト単位の API メトリクス（JSONL）を、毎日 Apache Iceberg のテーブル
（silver / gold）に変換する Cloudflare Workflow です。**Workflows / Pipelines / R2 Data Catalog /
R2 SQL** だけで組んでおり、外部の計算資源や DWH は使いません。

English: [README.md](README.md)（正本）

- 規模: 1 日あたり bronze オブジェクト 6,878〜32,557 件（2026-09 時点）。240 日分を backfill 済み
- 重心は「リトライがあっても数が合うこと」。毎日、自分の件数を突き合わせたうえで、
  テーブルに実際に入った行を数え直す

## 流れ

```
R2 bronze (JSONL) ─ cron 01:00 UTC ─▶ MedallionDayWorkflow
  list → chunk-<n>（型付け・不正行は quarantine）→ gold（集計）→ manifest（件数照合）
  → verify（R2 SQL で数え直し）→ alert（Discord）
        │ Pipelines                          │ Pipelines
        ▼                                    ▼
  silver.api_metrics (Iceberg)        gold.api_metrics_daily (Iceberg)

cron 02:00 UTC ─ 前日の manifest を読み直すヘルスチェック
```

## 設計判断

**`list` はキーではなく「チャンク記述子」を返す。** Workflows はステップの戻り値を永続化するので、
3 万件強のキーをそのまま返すと状態サイズの上限に近づく（正確な上限はドキュメントで見つけられなかった）。
約 2,000 件ごとの `(service, prefix, cursor, 件数)` だけを返し、各チャンクが自分で列挙し直す。

**`latency_ms = 0` はレイテンシ統計から外す。** Workers の `Date.now()` は I/O をするまで進まないので、
I/O なしの応答（404・認証の早期拒否など）は 0 ms になる。混ぜると p50 が 0 に張り付き、実際の処理の
遅さが見えなくなった（2026-09-21 の実データ）。

**gold では 404 を `(unmatched)` にまとめる。** スキャナが叩く `/.env` などで、9/21 は gold 1,070 行の
大半が 1 回きりのパスだった。生のパスは silver に残す。

**percentile は厳密値。** 1 日 3 万行強なら丸ごとソートしても安い。

**件数を 3 通りで突き合わせる。** bronze 行数 = silver + quarantine、silver = gold の requests 合計、
列挙したオブジェクト数 = 読めたオブジェクト数。3 つ目が無いと、消えたオブジェクトが「0 行」として
黙って通ってしまう。

**重複は防ぎきれない前提で、見分けて消せる形にする。** 240 日分の backfill 後、silver が manifest より
27,282 行多かった（240 日中 12 日で 1 チャンク分）。`send()` 成功後、ステップ結果の永続化前に
`WorkflowInternalError` で落ち、リトライがチャンクを送り直していた。リトライされるステップは複数回
走りうるので、ステップは冪等に書く必要がある。R2 への `put` は同じキーに同じ中身を書くだけなので冪等だが、
Pipelines の `send()` は追記のみで冪等ではない。そこで、こちら側で対処した:

1. 2,000 行ごとに進捗マーカーを R2 に書き、リトライは続きから送る（重複の区間を縮める。ゼロではない）
2. 行ごとに `row_uid`（元キー + 行番号）を持たせ、重複を機械的に見分けられるようにする
3. 毎日 `verify` が `count(*)` と `count(DISTINCT row_uid)` を比べる。`verify` 自体は throw しない
   （throw するとリトライされ、それが重複の原因になるため）

重複が出た日は `scripts/dedupe_day.py`（PyIceberg）で消す。R2 SQL は読み取り専用のため。

**通知の経路は 2 本。** Workflow 内の `alert` と、manifest が書かれなかった日を拾う 02:00 の cron。

## プラットフォームで観測したこと（2026-09 時点）

製品が変わっていく前提で、日付付きで残します。間違いがあればご指摘ください。

- sink が書くファイルは、テーブルのパーティション指定（`dt`）ではなく取り込み時刻で分かれていた
- パーティション指定を変えたら、R2 SQL のクエリがすべて `Query spans multiple partition specifications`
  で拒否された。既定の spec を 0 に戻して復旧
- PyIceberg の書き込みがエラーを返しても commit 済みのことがあった（2026-09-24）
- R2 SQL の REST レスポンスの形はドキュメントに例が無く、
  [cloudflare/skills](https://github.com/cloudflare/skills/blob/main/skills/cloudflare/references/r2-sql/api.md)
  で確認した

## 使い方

セットアップと運用のコマンドは [README.md](README.md#setup) にあります（英語版と共通）。

1 日をやり直すときは、その日の行を Spark か PyIceberg で消してから
`POST /run?dt=<dt>&attempt=2&force=1` を叩きます。Workflow の `create()` は同じインスタンス ID に
対して冪等ではない（2 回目はエラー）ので、`attempt` で別の ID を作ります。

```bash
pnpm install && pnpm typecheck && pnpm test
```

## ライセンス

MIT
