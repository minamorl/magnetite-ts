# magnetite の接続模型

通信を行わない、片側ごとの状態遷移模型。`step` は状態と出来事から新しい状態と effect を返す。`socket` は試行ごとに異なる番号を与える。フレームを届けるか捨てるか、両端の時計、再開の照合、新規接続や `transfer` を使った後継接続の生成は呼び出し側が担う。識別子と送受信の記録は一体であり、識別子だけが失われる状態は表現しない。

`ready` は client の識別子受領・ready 送出、または server の ready 受領を表す。`attempt` は client が再開要求を送信したこと、`resume` は server の要求受理または client の成功応答を表す。`resume` の連番は相手が連続して受け取り口に渡した最後の連番である。`replace` は外部で妥当性を判定済みの置換を表し、開いている server の `resume` からも置換を起こす。server への未知の識別子は closed 状態の `resume` で表す。時計の `tick` が飛び越えた ack 期限は effect の `at` に記録する。呼び出し側が effect を期限順に処理する離散時間模型である。

表を更新するには `node docs/generate.mjs`。試験は `node test/model.mjs <seed> <count>`。`MUTANT=delivery|notice|retention|terminal|ack|liveness|silent` で故障を注入できる。型検査は TypeScript の `tsc --noEmit --strict --target es2022 --module nodenext --moduleResolution nodenext src/model.ts`。

乱数網は方向ごとの FIFO、片側の切断、フレーム・再開要求・返事の喪失、独立した時計、置換、close と終了後の新規接続を扱う。喪失した socket の後続フレームも落とす。出力にはマスごとの回数とゼロを含む。反例は同じ assertion が落ちる範囲で一件ずつ削除し、これ以上一件を削れない列を表示する（大域的な最短列とは限らない）。

限界: 乱数では準備中から始めていない。保証中の欠けなしの完全照合は、一度も通信が途切れていない両端が開の実行に限る。切断後の未完了の通信を必ず再開して配送し切る検査はまだない。通知件数は個々の送信ではなく接頭性を前提に未配送件数と比較する。10 秒と期限の正確な境界は固定例で扱う。乱数と固定例は証明でも実通信の試験でもない。
