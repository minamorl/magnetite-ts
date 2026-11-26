# 口からの状態表対応 — magnetite v1.5

各セルは `magnetite.lifecycle.<列の状態>.<行の出来事>`。T は
`adapter.test.mjs` の下記の名前の試験、L は `loopback.test.mjs`。
U は口の試験で直接確かめていないマスとその理由。核の
`lifecycle.test.mjs` には 64 マスそれぞれの試験があるが、核の試験を
口の試験として数えない。

| 出来事 | preparing | open | disconnected | closed |
|---|---|---|---|---|
| ready | T1, T2 | U1 | U1 | U2 |
| send | T1 | T3, L1 | T4 | U3 |
| ack | T5 | T6, T7 | U4 | U2 |
| data | T8 | T3, T9 | U4 | U2 |
| ping | T8 | T10 | U4 | U2 |
| pong | T8 | T10 | U4 | U2 |
| loss | T11 | T4, T12, L1 | T13, T14 | U2 |
| attempt | U1 | U1 | T13, T14 | U1 |
| resume | T15 | T16, T17, T18 | T4, T19, L1 | U5 |
| reject | T8 | T18 | T20, L2 | U2 |
| expiry | U1 | U1 | T13, T14 | U1 |
| close | U6 | T3, T21, L1 | U6 | U3 |
| peerClose | U6 | T21 | U6 | U2 |
| replace | U1 | T16, T17, T19 | U1 | U1 |
| noIdentifier | T1, T3 | U7 | U7 | U7 |
| tick | T22, T23 | T10, T12, L3 | T13, T14 | U1 |

試験名:

- T1: `v1.5 server waits for ready and refuses sends until ready`
- T2: `v1.5 client sends ready before opening`
- T3: `injected sockets: handshake, bidirectional frames, ack and close`
- T4: `injected sockets: lost socket ignored and retained send resumes`
- T5: `injected sockets: ack while preparing is a protocol error`
- T6: `injected sockets: cumulative ack frees the configured send window`
- T7: `injected sockets: invalid sequence closes and reports pending`
- T8: `injected sockets: preparing ignores <data|ping|pong|reject>`
- T9: `injected sockets: duplicate is acked without duplicate delivery`
- T10: `injected sockets: ping and pong leave delivery sequence unchanged`
- T11: `injected sockets: loss while preparing reports connect_failed`
- T12: `injected sockets: ping and silent loss fire at configured deadlines between periodic ticks`
- T13: `injected sockets: retry continues until expiry and reports each retained send`
- T14: `injected sockets: a stalled attempt is closed at ten seconds after expiry`
- T15: `v1.5 preparing server ignores resume instead of accepting it`
- T16: `injected sockets: resume of active server replaces old connection`
- T17: `v1.5 resume reply precedes replaced notification and successor effects`
- T18: `v1.5 invalid resume on current socket only rejects and refreshes liveness`
- T19: `injected sockets: lost resume reply replaces server connection and preserves messages`
- T20: `injected sockets: restarted server rejects and reports unacknowledged count`
- T21: `injected sockets: peer close reports the pending count once`
- T22: `v1.5 client deadline starts at request and not socket creation`
- T23: `v1.5 server preparation deadline does not move on handshake frames`
- L1: `real WebSocket: round trip, loss, resume and close`
- L2: `real WebSocket: restarted server rejects resume with pending count`
- L3: `real WebSocket: silent TCP proxy triggers loss and resume`

未確認の理由:

- U1: その状態の出来事を外から直接注入する公開口がなく、当該の無視を口経由で個別に観測する試験がない。
- U2: 終了済み socket の callback を明示的に発火して、そのマスを個別に確認する試験がない。
- U3: 呼び出し側 API をその状態で呼ぶ個別試験がない。
- U4: 切断中の旧 socket の当該 frame を個別に注入する試験がない。再開後の旧 socket では各 frame を試している。
- U5: 終了済み識別子に対する要求を、同じ server の口から個別に試す試験がない。再起動後の unknown identifier は L2。
- U6: その状態での close または peer close の結果を個別に検証する試験がない。
- U7: 旧接続がその状態のまま識別子なし要求を新しい socket から受ける試験がない。初回の識別子なし要求は T1。
