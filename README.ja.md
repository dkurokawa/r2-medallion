# r2-medallion（日本語版）

R2 に置かれたリクエスト単位の API メトリクス（JSONL）を、毎日 **Apache Iceberg** のテーブルに
変換する Cloudflare Workflow です。bronze → silver → gold のメダリオンを、
**Workflows / Pipelines / R2 Data Catalog / R2 SQL** だけで組んでいます。外部の計算資源や
別の DWH は使いません。

English: [README.md](README.md)（こちらが正本です。内容は同じですが、差があれば英語版を優先します）

重心は **リトライがあっても数が合うこと** に置いています。毎日の実行で自分の件数を突き合わせ、
さらにテーブルに実際に入った行を数え直して重複を検出します。以下の設計判断は、推測ではなく
実データで観測した数字から決めています。

- 規模: 1 日あたり bronze オブジェクト 6,878〜32,557 件（2026-09 時点）。240 日分の backfill を流しています
- TypeScript（Workers ランタイム）、Vitest の単体テスト 144 件。修復用に PyIceberg のスクリプト

---

## 構成

```
R2 バケット（bronze）
  api-metrics/<dt>/<service>/<hour>/<file>.jsonl     ← 上流が書く。ほぼ 1 オブジェクト = 1 リクエスト
        │
        │ cron 01:00 UTC → MEDALLION_WORKFLOW.create({ id: "day-<dt>", params: { dt } })
        ▼                  （手動なら POST /run?dt=...）
┌──────────────────────────── MedallionDayWorkflow ────────────────────────────┐
│ guard     _state/medallion/dt=<dt>.json が done なら終了                      │
│ list      api-metrics/<dt>/ をサービスごとに列挙 → 軽い「チャンク記述子」     │
│           （service, prefix, 再開用 cursor, 件数）だけを返す                 │
│ chunk-<n> 記述子から再列挙 → get → JSONL を行ごとに検証・型付け               │
│             ├ 不正な行 → quarantine/api-metrics/dt=<dt>/chunk-<n>.jsonl      │
│             └ 正常な行 → SILVER_STREAM.send()（再開可能。下記）              │
│           同時に (service, endpoint, method) ごとの部分集計を返す             │
│ gold      部分集計をマージ → 厳密な percentile → GOLD_STREAM.send()           │
│ manifest  3 通りの件数突き合わせ → _state/medallion/dt=<dt>.json              │
│ sleep     6 分（Pipelines の sink がファイルを確定させるのを待つ）            │
│ verify    R2 SQL で silver・gold の count(*) と count(DISTINCT row_uid)      │
│ alert     問題があれば Discord に通知                                         │
└──────────────────────────────────────────────────────────────────────────────┘
        │ SILVER_STREAM                         │ GOLD_STREAM
        ▼ Pipelines → R2 Data Catalog sink      ▼ Pipelines → R2 Data Catalog sink
  silver.api_metrics（Iceberg）           gold.api_metrics_daily（Iceberg）
        └──────────────── R2 SQL で参照（読み取り専用）───────────────┘

cron 02:00 UTC → 独立したヘルスチェック。前日の manifest を読み直し、
                 Workflow が manifest を書かずに終わった日（落ちた・起動しなかった等）を通知
```

この Worker は bronze を**読むだけ**で、上流の書き手が出したものには手を加えません。

## 設計判断

### 1. `list` ステップはキーではなくチャンク記述子を返す

Workflows は `step.do` の戻り値をインスタンスの状態として永続化します。1 ステップで 3 万件強の
キーを返すと、永続化できる状態サイズの上限に近づくおそれがあります（正確なバイト上限は
ドキュメントで見つけられませんでした）。そこで `list` は、約 2,000 件ごとに区切った数十件の記述子
`(service, prefix, 再開用 cursor, 件数)` だけを返し、各 `chunk-<n>` が cursor から自分の担当分を
列挙し直します。増える `list()` は 1 日数十回の Class A 操作で、3 万件強の `get()` に比べれば
無視できます。→ `src/lib/r2-list.ts`

### 2. percentile はヒストグラム近似ではなく厳密値

1 日は多くて 3 万行強なので、丸ごと 1 回ソートしても安く済みます。gold の
`latency_p50/p95/p99` は、全チャンクの部分集計をマージしたうえでの厳密値（nearest-rank 法）です。
→ `src/lib/percentile.ts`, `src/lib/aggregate.ts`

### 3. `latency_ms = 0` はレイテンシ統計から外す

実データ（2026-09-21）で気づいた点です。Workers の `Date.now()` は isolate が I/O をするまで
進まないため、I/O なしで返す応答（静的な応答・404・認証の早期拒否）は 0 ms と記録されます。
計測漏れではありませんが、percentile に混ぜると p50 が 0 に引っぱられ、実際に処理をした
リクエストの遅さが見えなくなりました。gold は 0 ms 以外の行からレイテンシを計算し、内訳を
`requests_measured` と `requests_zero_latency` に分けて持ちます（計測した行が無いときは
`0` ではなく `null`）。

### 4. gold では 404 を `(unmatched)` にまとめる

脆弱性スキャナが `/.env` `/wp-login.php` `/phpinfo.php` などを叩きに来ます。2026-09-21 は
gold 1,070 行の大半が、こうした 1 回きりのパスでした。404 になったパスはそのサービスの
エンドポイントではないので、gold では `(unmatched)` にまとめます。silver には生のパスが
残るので、どこが叩かれたかは silver で追えます。

### 5. 毎日、自分の件数を 3 通りで突き合わせる

`manifest` ステップの検証:

| 突き合わせ | 検出するもの |
|---|---|
| bronze の JSONL 行数 = silver 行数 + quarantine 行数 | パースで落ちた行 |
| silver 行数 = gold の `requests` 合計 | 集計のずれ |
| 列挙したオブジェクト数 = 読めたオブジェクト数（`get()` が `null` の件数 = 0） | list と get の間に消えた bronze |

3 つ目が要です。これが無いと、消えたオブジェクトは「0 行のオブジェクト」として黙って数えられ、
残り 2 つの式はそのまま成り立ってしまいます。不一致のときは、ステップが throw する**前に**
`status: "mismatch"` の manifest を書くので、証跡が残ります。→ `src/lib/manifest.ts`

### 6. リトライは at-least-once。重複は見分けられて消せる形にする

起きたこと: 240 日分の backfill が manifest の検証を全部通ったあと、R2 SQL で数えると
**silver が manifest の記録より 27,282 行多い**状態でした。240 日中 12 日で、ちょうど 1 チャンク分が
重複していました。`wrangler workflows instances describe` で見ると、`chunk-<n>` ステップが
`SILVER_STREAM.send()` を終えたあと、ステップの結果が永続化される前に
`WorkflowInternalError: Attempt failed due to internal workflows error` で失敗し、
自動リトライがチャンク全体を送り直していました。Pipelines は追記のみなので、行はそのまま残ります。

リトライされるステップは複数回走りうる、というのは通常の前提なので、こちら側で 3 段で対処しました。

1. **重複が起きる区間を縮める。** 各チャンクは 2,000 行ずつ送り、1 回送るたびに進捗マーカーを
   R2 に書きます（Workflow のインスタンス ID ごと）。リトライ時はマーカーを読んで、送信済みの
   続きから再開します。重複が起きうる区間は「ステップ全体（最大 15 分）」から
   「1 回の `send()` が成功してから、小さな R2 `put()` が終わるまで」に縮みます。**ゼロではありません**。
   → `src/lib/chunk-marker.ts`
2. **すべての行を識別できるようにする。** silver は `row_uid = <元の R2 キー>#<行番号>`、
   gold は `row_uid = <dt>|<service>|<endpoint>|<method>`。同じ元の行は何度送っても同じ
   `row_uid` になります。
3. **毎日テーブルを数え直す。** sink がファイルを確定させたあと、`verify` ステップがその日の
   silver・gold に `SELECT count(*), count(DISTINCT row_uid)` を投げ、結果を manifest に残します。
   distinct がその実行自身の記録と食い違えば `mismatch`、`count(*) > distinct` なら `duplicates`。
   `verify` は throw しません。throw するとステップがリトライされ、それこそが重複の原因になるためです。

重複が見つかった日は `scripts/dedupe_day.py`（PyIceberg）で消します（R2 SQL は読み取り専用のため）。
消すまでの間は
`QUALIFY row_number() OVER (PARTITION BY row_uid ORDER BY __ingest_ts) = 1` で重複を除いて読めます。

### 7. 通知の経路を 2 本、独立に持つ

- Workflow の `alert` ステップ: `duplicates` と `verify` のエラーを拾う
- 02:00 UTC の 2 本目の cron: 前日の manifest を読み直し、Workflow が manifest を書かずに終わった日を拾う

どちらも同じ `manifestProblems()` で判定します。悪い日は通知が 2 通になりえますが、意図どおりです
（通知の取りこぼしのほうが悪い）。`notifyDiscord` は throw しないので、webhook が壊れていても
パイプラインは止まりません。→ `src/lib/alert.ts`, `src/index.ts`

### 8. 管理用エンドポイントは閉じる側に倒す

`POST /run` と `GET /status` は `Authorization: Bearer <ADMIN_TOKEN>` が必要です。secret が
未設定なら、通すのではなく全部 401 で断ります。比較は定数時間です（両辺を先に SHA-256 で
ハッシュする）。→ `src/lib/auth.ts`

## プラットフォームで観測したこと（2026-09 時点）

作りながら観測した挙動です。製品が変わっていく前提で、日付を付けて残します。間違いがあれば
ご指摘ください。

- **sink が書くファイルは、テーブルのパーティション指定ではなく取り込み時刻で分かれる。**
  2026-09-25 に `dt` の identity パーティションを足しましたが、Pipelines の sink が書くファイルは
  取り込み時刻（`__ingest_ts`）で分かれたままでした
- **パーティション指定を変えたら、R2 SQL からテーブルを引けなくなった。** 既定の spec と既存
  manifest の spec が食い違い、すべてのクエリが `Query spans multiple partition specifications`
  で拒否されました。既定の spec を 0 に戻して復旧しています
- **PyIceberg の書き込みは、エラーを返しても commit 済みのことがある**（2026-09-24）。
  エラーを見たら、まず `dedupe_day.py count <dt>` でその日の現状を確かめます
- **R2 SQL の REST レスポンスの形**: エンドポイントは
  [Query data](https://developers.cloudflare.com/r2-sql/query-data/) に載っていますが、
  レスポンスの例は無かったため、フィールド名（`result.rows` / `schema` / `metrics`, `success`, `errors`）は
  [cloudflare/skills](https://github.com/cloudflare/skills/blob/main/skills/cloudflare/references/r2-sql/api.md)
  で確認しました。`src/lib/r2sql.ts` はこれに合わせています

## テーブル

**`silver.api_metrics`**（1 行 = 1 リクエスト）:
`ts`, `dt`, `service`, `endpoint`, `method`, `status`, `status_class`, `latency_ms`,
`request_size`, `response_size`, `user_id`, `cf_ray`, `cf_colo`, `error_message`,
`source_key`, `row_uid`。`service` はペイロードではなく R2 キーのパスから取ります
（ペイロードの中身より、bronze の物理配置を信頼する）。

**`gold.api_metrics_daily`**（`dt × service × endpoint × method` ごとに 1 行）:
`requests`, `errors_4xx`, `errors_5xx`, `error_rate`, `requests_measured`,
`requests_zero_latency`, `latency_p50/p95/p99/avg/max`, `colos`, `row_uid`。
`colos` はソート済みのカンマ区切り文字列です（Pipelines のスキーマファイルが配列型に対応するか
確認できなかったため、確実に通る型にしています）。

スキーマ: [`schema/silver.json`](schema/silver.json), [`schema/gold.json`](schema/gold.json)

## セットアップ・運用

手順（ストリーム・sink・パイプラインの作成、secret、デプロイ、やり直し、重複の除去）は
[README.md の Setup / Operations](README.md#setup) を見てください。コマンドは英語版と共通です。

要点だけ:

- Pipelines は追記のみ、R2 SQL は読み取り専用です。1 日をやり直すときは、先にその日の行を
  Spark か PyIceberg で消してから `POST /run?dt=<dt>&attempt=2&force=1` を叩きます
- Workflow の `create()` は同じインスタンス ID に対して冪等ではないので、`attempt=N` で
  別の ID（`day-<dt>-r<N>`）を作ります。重複を防いでいるのは `_state` の完了記録で、
  インスタンス ID ではありません
- `dedupe_day.py dedupe` のあとは、必ず `verify-all` で全日を manifest と照合します

## 開発

```bash
pnpm install
pnpm typecheck
pnpm test        # 144 件。`cloudflare:workers` は Node 用のスタブに差し替えています
```

## ライセンス

MIT
