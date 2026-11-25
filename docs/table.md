# 状態遷移表

`node docs/generate.mjs` で src/model.ts の実装済み matrix から生成。各行はイベントを処理する側。出来事は一列で処理する。

| 出来事 | 準備中 | 開 | 切断中 | 閉 |
|---|---|---|---|---|
| ready | 開へ、接続完了通知 | 無視 | 無視 | 無視 |
| send | not_open | 枠内なら受理し送出、超過は limit | 期限前は枠内で受理、期限後 not_open | closed |
| ack | 現 socket は protocol_error、旧 socket は無視 | 累積 ack を解放、最大超過は protocol_error | 無視 | 無視 |
| data | 無視 | 順に配送し ack を予約、重複は破棄 | 無視 | 無視 |
| loss | connect_failed、識別子破棄 | 切断中へ、期限開始 | 試行だけ終了し再試行、期限後は expired | 無視 |
| attempt | 無視 | 無視 | client の再開試行開始、10 秒待つ | 無視 |
| resume | 無視 | server は無効なら拒否し旧接続維持、有効なら記録を引き継ぎ replaced | 有効なら開き累積 ack と再送、期限境界を判定 | server は未知の識別子として拒否 |
| reject | 無視 | 無視 | client は resume_rejected | 無視 |
| expiry | 無視 | 無視 | 待機中は応答または 10 秒まで保持 | 無視 |
| close | closed_by_caller、件数 0 | close frame と closed_by_caller | closed_by_caller | 成功、通知なし |
| peerClose | peer_closed、件数 0 | peer_closed | 現在の socket なら peer_closed | 無視 |
| replace | 無視 | 記録を引き継ぎ replaced 通知 | 無視 | 無視 |
| noIdentifier | server は新規接続 | server は旧接続を保ち新規接続 | server は別の新規接続、旧記録を維持 | server は新規接続 |
| tick | 時計を進める | 時計と ack 期限 | 10 秒または期限を判定 | 時計のみ進む |

14 events × 4 states = 56 cells; empty: 0
