# @minamorl/magnetite

Reliable WebSocket connections for TypeScript (Node.js and browser clients, Node.js servers) with zero runtime dependencies.

Built from the `magnetite` v1.5 state-machine specification: a pure protocol core (`Core`) paired with a thin WebSocket client/server adapter (`Client`, `connect`, `Server`, `ServerConnection`).

## What It Promises

Within a session that reconnects before its disconnect expiry window and without process restarts:

- **Zero message loss and zero duplicates across reconnects**: Every message accepted by `send()` receives a strictly increasing per-direction sequence number and stays buffered in memory until cumulatively acknowledged (`ack`) by the peer after contiguous delivery to `onReceive`. Retransmitted duplicates are acknowledged and discarded without duplicate delivery. Send order per direction is preserved across reconnects within the same connection identifier.
- **Resumable sessions**: On initial connection, the server issues an unpredictable UUIDv7 connection identifier. After a transport drop, the client reconnects with exponential backoff and jitter (default 0.5 s doubling up to a 10 s cap, with 0–10% jitter never exceeding 10 s), sends its identifier and last contiguously received sequence number, and resumes without losing buffered messages on either side.
- **Silent-link detection**: On an open socket, receiving no frames for `pingAfter` (default 15 s) triggers a single `ping`; the peer replies with `pong` within 1 s. Receiving no frames for `lossAfter` (default 30 s) closes the socket as lost and transitions to recovery. (`ping` and `pong` do not affect sequence numbers, send limits, or delivery order.) The initial `preparing` handshake also fails with `connect_failed` after `lossAfter`.
- **No silent drops — explicit rejection and unacknowledged counts**:
  - Each direction enforces a configurable unacknowledged send buffer limit: default **1,000 messages** or **8 MiB** (measured as UTF-8 byte length of the serialized JSON `data` frame), whichever comes first. Sends past the limit return `{ result: 'limit', effects: [] }` without discarding previously accepted messages.
  - Sends while `preparing` (or while `disconnected` at or past `expiryAfter`) return `{ result: 'not_open', effects: [] }`; sends while `closed` return `{ result: 'closed', effects: [] }`.
  - When a connection terminates (`connect_failed`, `protocol_error`, `expired`, `resume_rejected`, `closed_by_caller`, or `peer_closed`), `onEnd` is called once with `{ reason, unacked }`, reporting the exact number of unacknowledged outgoing messages discarded. (When an active server connection is superseded by a valid resume on a new socket, it terminates with `{ reason: 'replaced' }` and no `unacked` count because its pending record transfers to the successor `ServerConnection`.)

## What It Does Not Promise

- **Surviving process restarts or crashes**: Pending messages, session identifiers, and deduplication state are kept in memory only. If one process restarts, its session state is lost; the surviving peer is notified once with `resume_rejected` (if it attempts to resume) or `expired` (if `expiryAfter` elapses first) along with its `unacked` count. If both processes restart, neither emits a notice for the lost session.
- **Recovery after `expiryAfter`**: Unresumed disconnected state is discarded once elapsed time since disconnect reaches `expiryAfter` (default 10 minutes; a client with an in-flight resume attempt waits up to 10 s for that attempt to settle or time out) and reported via `onEnd({ reason: 'expired', unacked })`.
- **Durability after `onReceive`**: Delivery is counted when the receiver library passes a message to `onReceive`. Processing or persisting it inside your callback is the application's responsibility.
- **Authentication, authorization, or transport encryption**: Use `wss://` and authenticate during the HTTP/WebSocket upgrade before passing sockets to `Server.accept(socket)`.
- **Unbounded queuing**: Messages rejected with `'limit'`, `'not_open'`, or `'closed'` are not queued.
- **Multi-process session sharing**: `Server` tracks sessions in an in-memory `Map`. Multi-node deployments require sticky routing or a custom transport over `@minamorl/magnetite/core`.
- **Binary payloads**: Wire frames carry JSON values (`Json`).

## Installation

Requires Node.js 22 or newer (or a browser environment with global `WebSocket` and `crypto.getRandomValues` for the client).

```bash
npm install @minamorl/magnetite
```

On Node.js servers, pair `Server` with any `SocketLike` WebSocket server implementation such as [`ws`](https://github.com/websockets/ws):

```bash
npm install ws
```

## Minimal Usage

### Server (Node.js with `ws`)

```ts
import { WebSocketServer } from 'ws';
import { Server } from '@minamorl/magnetite';

const wss = new WebSocketServer({ port: 8080 });
const server = new Server({
  onConnection(conn) {
    conn.setCallbacks({
      onReceive(data, seq) {
        console.log('received from client', seq, data);
        const res = conn.send({ echo: data });
        if (res.result !== 'accepted') {
          console.warn('send rejected:', res.result);
        }
      },
      onState(state) {
        console.log(conn.identifier, 'state:', state);
      },
      onEnd(notice) {
        console.log(conn.identifier, 'ended:', notice);
      },
    });
  },
});

wss.on('connection', (socket) => {
  server.accept(socket);
});
```

### Client (Node.js 22+ or Browser)

```ts
import { connect } from '@minamorl/magnetite';

let greeted = false;
const client = connect('ws://127.0.0.1:8080', {
  onState(state) {
    console.log('state:', state);
    if (state === 'open' && !greeted) {
      greeted = true;
      const res = client.send({ hello: 'world' });
      if (res.result !== 'accepted') {
        console.warn('send rejected:', res.result);
      }
    }
  },
  onReceive(data, seq) {
    console.log('received from server', seq, data);
  },
  onEnd(notice) {
    // notice: { reason, unacked? }
    console.log('connection ended:', notice);
  },
});

// Later, to close cleanly:
// client.close();
```

### Pure State-Machine Core

If you want to integrate a custom transport or run deterministic simulations without timers or sockets, import `Core` from the root package or the `@minamorl/magnetite/core` subpath:

```ts
import { Core, issueIdentifier } from '@minamorl/magnetite/core';

const id = issueIdentifier(Date.now(), (n) => crypto.getRandomValues(new Uint8Array(n)));
const core = new Core({ role: 'server', identifier: id, now: 0, socket: 's1' });
const { effects } = core.dispatch({ type: 'ready', socket: 's1' });
// Execute returned effects (frames, closeSocket, connected, deliver, retry, ended, ...)
```

## Configuration & States

All timing and capacity options are optional and apply to `Client`, `Server`, and `Core`:

| Option | Default | Scope | Description |
|---|---:|---|---|
| `expiryAfter` | `600000` (10 min) | Client / Server / Core | Time a disconnected session is retained before expiring (ms). |
| `pingAfter` | `15000` (15 s) | Client / Server / Core | Idle time without received frames before sending `ping` (ms). |
| `lossAfter` | `30000` (30 s) | Client / Server / Core | Idle time without received frames before declaring socket loss, or `preparing` handshake timeout (ms). |
| `maxCount` | `1000` | Client / Server / Core | Maximum unacknowledged outgoing messages per direction. |
| `maxBytes` | `8388608` (8 MiB) | Client / Server / Core | Maximum total UTF-8 bytes of unacknowledged JSON `data` frames per direction. |
| `tickInterval` | `min(250, pingAfter/4, lossAfter/4)` | Client / Server | Periodic clock-advance interval (ms); exact deadlines are also scheduled with timers. |
| `retryInitial` | `500` | Client | Initial reconnect backoff base delay (ms). |
| `retryMax` | `10000` | Client | Maximum reconnect backoff delay (ms, capped at 10,000 ms). |
| `socketFactory` | `(url) => new WebSocket(url)` | Client | Custom `SocketLike` factory. |
| `clock` | `systemClock` | Client / Server | Injectable clock (`now`, `setTimeout`, `clearTimeout`) for deterministic tests. |
| `random` | `Math.random` | Client | Random source in `[0, 1]` for reconnect jitter. |
| `randomBytes` | `crypto.getRandomValues` | Server | Cryptographically unpredictable bytes for UUIDv7 session identifiers. |

- **Connection states (`State`)**: `'preparing' | 'open' | 'disconnected' | 'closed'`
- **Send results (`Result['result']`)**: `'accepted'` (with `seq`), `'limit'`, `'not_open'`, `'closed'`
- **End reasons (`EndReason`)**: `'connect_failed' | 'protocol_error' | 'expired' | 'resume_rejected' | 'closed_by_caller' | 'peer_closed' | 'replaced'`

## Specification Mapping (`magnetite` v1.5)

The implementation and tests map directly to the `magnetite` v1.5 specification:

| Spec Area | Implementation | Tests & Artifacts |
|---|---|---|
| Lifecycle transition matrix (`magnetite.lifecycle.*`: 4 states × 16 events = 64 cells, retired sockets) | [`src/core.ts`](src/core.ts) | [`test/lifecycle.test.mjs`](test/lifecycle.test.mjs), [`test/v14.test.mjs`](test/v14.test.mjs), [`test/model.ts`](test/model.ts) (executable oracle), [`test/model.mjs`](test/model.mjs), [`docs/table.md`](docs/table.md) |
| Initial & resume handshake (`magnetite.handshake.*`, `magnetite.protocol.*`) | [`src/client.ts`](src/client.ts), [`src/server.ts`](src/server.ts), [`src/transport.ts`](src/transport.ts) | [`test/adapter.test.mjs`](test/adapter.test.mjs), [`test/loopback.test.mjs`](test/loopback.test.mjs), [`test/adapter-coverage.md`](test/adapter-coverage.md) |
| Delivery guarantees, ordering, deduplication, retention limits (`magnetite.delivery.*`, `magnetite.retention.*`, `magnetite.ordering.*`, `magnetite.model.*`) | [`src/core.ts`](src/core.ts) | [`test/lifecycle.test.mjs`](test/lifecycle.test.mjs), [`test/network.test.mjs`](test/network.test.mjs), [`test/model.mjs`](test/model.mjs) |
| Silent-link detection & liveness (`magnetite.liveness.*`) | [`src/core.ts`](src/core.ts), [`src/transport.ts`](src/transport.ts) | [`test/lifecycle.test.mjs`](test/lifecycle.test.mjs), [`test/v14.test.mjs`](test/v14.test.mjs), [`test/adapter.test.mjs`](test/adapter.test.mjs), [`test/loopback.test.mjs`](test/loopback.test.mjs) |
| Reconnect backoff & jitter (`magnetite.reconnect.*`) | [`src/client.ts`](src/client.ts) | [`test/adapter.test.mjs`](test/adapter.test.mjs) |
| Fault injection / mutation checks | [`scripts/mutations.mjs`](scripts/mutations.mjs), [`scripts/adapter-mutations.mjs`](scripts/adapter-mutations.mjs) | Core, adapter, and model oracle (`MUTANT=... node test/model.mjs`) |

`test/model.ts` is kept strictly as a non-published test oracle and transition-table source; only the compiled `Core` and WebSocket adapter under `dist/` are exported and packed.

## Development

```bash
npm ci
npm run typecheck
npm run build
npm test
npm run test:model
npm run pack:check
npm run mutations
npm run mutations:adapter
```

## License

[MIT](LICENSE)
