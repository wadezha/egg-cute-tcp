# egg-cute-tcp

A TCP server plugin for [Egg.js](https://eggjs.org). It accepts raw TCP connections from devices, runs incoming and outgoing data through a chain of handlers you write, and keeps track of device sessions across workers and machines using Redis plus a Pub/Sub channel (Redis or RabbitMQ).

## Features

- One or several TCP servers per app, each with its own port, protocol, parser and handler chains.
- Pluggable **in chain** (device → server) and **out chain** (server → device) handlers.
- Session management: one session per connection, registered by a device key (for example an IMEI).
- Cluster aware: `sessionHub.send()` reaches a device even when it is connected to another worker or machine, and a device that reconnects elsewhere has its stale session kicked automatically.
- Idle timeout, TCP keep-alive, optional per-connection ordered dispatch.
- Pub/Sub backend is switchable: Redis or RabbitMQ.
- TypeScript typings included.

## Requirements

- Node.js >= 18
- The following Egg plugins must be enabled:
  - [`egg-redis`](https://github.com/eggjs/egg-redis) (session cache, and Pub/Sub when `pubSubType` is `redis`)
  - A snowflake plugin named `egg-cute-snowflake` that provides `app.snowflake.nextId()` (used to generate session ids)
  - `egg-rabbitmq` (exposes `app.rabbit`), only when `pubSubType` is `rabbit`

## Install

```bash
npm i egg-cute-tcp --save
```

## Enable the plugin

```js
// config/plugin.js
exports.tcp = {
  enable: true,
  package: 'egg-cute-tcp',
};
```

## Quick start

### 1. Configure a single TCP server

```js
// config/config.default.js
exports.tcp = {
  app: true,
  pubSubType: 'redis',
  pubSubChannel: 'cute-tcp:pub_sub',
  eventSend: 'client.send',
  eventKickout: 'client.kickout',
  client: {
    port: 1883,
    protocol: 'ect',
    enableDispatchQueue: false,
    idleTimeout: 300,
    keepAlive: 60 * 1000,
    sessionKeyCacheKey: 'tcp:ect:session:key',
    sessionIdCacheKey: 'tcp:ect:session:id',
    chainPath: 'app/tcp/chain',
    parserPath: 'app/tcp/parser',
    inChain: ['auth', 'dispatcher'],
    outChain: ['encrypt'],
  },
};
```

All other options have defaults, see [Configuration](#configuration).

### 2. Write handlers

Handlers live in the directory set by `chainPath` (relative to the app's base directory).

```js
// app/tcp/chain/auth.js
const { Chain } = require('egg-cute-tcp');

class Auth extends Chain {
  async handle(session, message, sessionHub) {
    if (!session.sessionKey) {
      // parse the device key (for example the IMEI) from the login packet
      session.sessionKey = '...';
      await sessionHub.register(session);
    }
    return { message };
  }
}

module.exports = Auth;
```

```js
// app/tcp/chain/dispatcher.js
const { Chain } = require('egg-cute-tcp');

class Dispatcher extends Chain {
  async handle(session, message, sessionHub) {
    this.ctx.logger.info('sessionId: %s, message: %s', session.sessionId, message);
    // business logic ...
    // return { break: true } to stop the remaining in-chain handlers
  }
}

module.exports = Dispatcher;
```

```js
// app/tcp/chain/encrypt.js (out chain)
const { Chain } = require('egg-cute-tcp');

class Encrypt extends Chain {
  async handle(session, message, sessionHub) {
    // `message` is whatever was passed to session.send() / sessionHub.send().
    // The last out-chain handler must return a Buffer or a string.
    return { message: Buffer.from(JSON.stringify(message)) };
  }
}

module.exports = Encrypt;
```

### 3. Send a message to a device

```js
// anywhere in your app, after the app is ready
const ok = await app.tcp.sessionHub.send(imei, { cmd: 'reboot' });
```

## Multiple TCP servers

Use `clients` instead of `client`.

```js
exports.tcp = {
  app: true,
  pubSubType: 'redis',
  pubSubClient: '',
  pubSubChannel: 'cute-tcp:pub_sub',
  eventSend: 'client.send',
  eventKickout: 'client.kickout',
  clients: {
    bsj: {
      port: 1882,
      protocol: 'bsj',
      enableDispatchQueue: false,
      idleTimeout: 300,
      keepAlive: 60 * 1000,
      sessionRedisClient: '',
      sessionKeyCacheKey: '',
      sessionIdCacheKey: '',
      parserPath: '',
      chainPath: '',
      inChain: [],
      outChain: [],
    },
    swd: {
      port: 1881,
      protocol: 'swd',
      enableDispatchQueue: false,
      idleTimeout: 300,
      keepAlive: 60 * 1000,
      sessionRedisClient: '',
      sessionKeyCacheKey: '',
      sessionIdCacheKey: '',
      parserPath: '',
      chainPath: '',
      inChain: [],
      outChain: [],
    },
  },
};
```

```js
const bsj = app.tcp.get('bsj');
await bsj.sessionHub.send(imei, message);
bsj.parser.cute.decode(hex);

const swd = app.tcp.get('swd');
await swd.sessionHub.send(imei, message);
```

`client` and `clients` are mutually exclusive: use one or the other.

## Configuration

### Global options

| Option | Default | Description |
| --- | --- | --- |
| `app` | `true` | Set to `false` to disable the plugin (no server, no Pub/Sub) without removing it. |
| `pubSubType` | `'redis'` | Pub/Sub backend: `'redis'` or `'rabbit'`. The matching plugin must be enabled. |
| `pubSubChannel` | `'cute-tcp:pub_sub'` | Redis channel name, or the name of the RabbitMQ fanout exchange. |
| `pubSubClient` | `''` | Name of a client in a multi-instance `egg-redis` / `egg-rabbitmq` setup, i.e. `app.redis.get('r1')` → `'r1'`. Leave empty when you use a single instance. |
| `eventSend` | `'client.send'` | `type` of the Pub/Sub message that sends data to a device. |
| `eventKickout` | `'client.kickout'` | `type` of the Pub/Sub message that kicks a device offline. |
| `client` | see below | A single TCP server. |
| `clients` | – | Several TCP servers, keyed by name. |

### Per-client options

| Option | Default | Description |
| --- | --- | --- |
| `port` | `1883` | TCP port to listen on. |
| `protocol` | `'ect'` | Device protocol identifier. Pub/Sub messages carry it, so devices of different protocols never interfere with each other. With `clients`, it is always the client's key and any configured value is overridden. |
| `enableDispatchQueue` | `false` | Process the messages of one connection strictly one after another. Guarantees ordering, but a burst of data from one device queues up in memory. |
| `idleTimeout` | `300` | Idle timeout in **seconds**; must be a positive integer. The connection is closed when no data has been received from the device for this long. Data sent by the server does not reset it. |
| `keepAlive` | `60000` | Initial delay of TCP keep-alive probes in **milliseconds**. |
| `sessionRedisClient` | `''` | Name of the `egg-redis` client that stores sessions, i.e. `app.redis.get('r1')` → `'r1'`. Leave empty for the default client. |
| `sessionKeyCacheKey` | – (required) | Redis key prefix for the map `sessionKey → sessionId`. Must be unique across clients. |
| `sessionIdCacheKey` | – (required) | Redis key prefix for the map `sessionId → session`. Must be unique across clients. |
| `parserPath` | `''` | Directory of parser classes, relative to the app base directory. |
| `chainPath` | `''` | Directory of handler classes, relative to the app base directory. |
| `inChain` | `[]` | Names of the in-chain handlers, executed in array order. |
| `outChain` | `[]` | Names of the out-chain handlers, executed in array order. |

A handler's name is the name Egg's loader gives its file: `dispatcher.js` → `dispatcher`, `in/auth.js` → `in.auth`.

## Handlers

An in-chain or out-chain handler extends `Chain` and implements:

```js
async handle(session, message, sessionHub)
```

- Handler instances are created once per client and shared by all connections, so keep them stateless. Store per-connection state on `session`.
- Return `undefined`, or an object `{ message, break }`:
  - `message`: replaces the message passed to the next handler.
  - `break: true`: stops the remaining handlers of this chain.
- If a handler **throws**, the error is logged and the chain is aborted:
  - in chain: the current message is dropped (the session's Redis keep-alive is still refreshed);
  - out chain: nothing is written to the socket and `send()` resolves to `false`.
- Whatever the out chain returns last must be a `Buffer` or a `string`.

### Sticky and split packets

The plugin hands you raw chunks exactly as they arrive from the socket. Framing is up to you; the recommended place is the first handler of the in chain. The channel provides `session.channel.frameBuf` to hold the unfinished frame. Do not `await` anything between reading and updating that buffer, otherwise two chunks received back to back can overwrite each other's state. A framing handler may return an array as `message` (zero or several frames) and let the following handlers iterate over it.

### Parsers

Classes extending `Parser` (`encode` / `decode`) in `parserPath` are instantiated and exposed as `app.tcp.parser.<name>` (or `app.tcp.get('<client>').parser.<name>`). The plugin never calls them itself; use them from your handlers.

## Sessions

Every connection gets a `session` with `sessionId` (generated by `app.snowflake`), `clientIp`, `clientPort`, `connectTime` and an empty `sessionKey`. It becomes addressable by device key only after you register it.

### `sessionHub.register(session)`

Call it after the device is authenticated and `session.sessionKey` is set. It:

1. maps `sessionKey → session` in this process;
2. writes both Redis records;
3. broadcasts an online event so that every other worker or machine drops its older session for the same key;
4. closes an older local session with the same key.

### `sessionHub.send(sessionKey, message)`

Runs the out chain and writes to the device.

- Device connected to this process: resolves `true` once the data was handed to the socket (`false` if the connection is gone or the out chain failed).
- Device connected elsewhere: the command is published over Pub/Sub and the worker that owns the connection sends it. `true` only means "published"; it does not mean the device received the data. `false` means the device has no online record in Redis, or an error occurred.

### Other methods

- `sessionHub.getSession(sessionKey)`: the local session, or `null`.
- `sessionHub.getKeys()`: device keys connected to this process.
- `session.send(message)`, `session.close()`.

## Clustering

### Pub/Sub messages

Every worker subscribes to `pubSubChannel`: a Redis channel, or a RabbitMQ non-durable fanout exchange with one exclusive temporary queue per worker. Programs that do not use this library can control devices by publishing JSON text to it:

```json
{ "type": "client.send", "protocol": "ect", "sessionKey": "<device key>", "message": { } }
```

```json
{ "type": "client.kickout", "protocol": "ect", "sessionKey": "<device key>" }
```

`type` must equal `eventSend` / `eventKickout`. `message` goes through JSON, so it must be JSON-serializable.

When a device registers on a new worker, an internal `client.connected` message makes the other workers close their older session of that device. Only sessions older than the new one are kicked, so a device that reconnects quickly cannot kick itself offline.

### Redis data

| Key | Value |
| --- | --- |
| `<sessionKeyCacheKey>:<sessionKey>` | the `sessionId` |
| `<sessionIdCacheKey>:<sessionId>` | the session as JSON |

The TTL is 1.5 × `idleTimeout`. It is refreshed when the device sends data, at most once every 0.5 × `idleTimeout`, so Redis never forgets a device whose connection is still alive.

### Redis Cluster

Both keys are used together in Lua scripts, so they must hash to the same slot. Put the protocol in a hash tag:

```js
sessionKeyCacheKey: 'tcp:{swd}:session:key',
sessionIdCacheKey: 'tcp:{swd}:session:id',
```

### RabbitMQ reconnects

After `egg-rabbitmq` re-establishes its connection, call `app.tcpPubSub.restart()` to resubscribe.

## Lifecycle

- The TCP servers and the Pub/Sub subscription start in `app.ready`. If the port cannot be bound or the subscription fails, the worker logs the error and exits with code 1.
- On shutdown the servers stop accepting connections and all sessions are closed.
- The plugin runs in app workers only (not in the agent).

## TypeScript

```ts
import { Chain } from 'egg-cute-tcp';

export default class Dispatcher extends Chain {
  async handle(session, message, sessionHub) {
    return { message };
  }
}
```

## Questions & Suggestions

Please open an issue [here](https://github.com/wadezha/egg-cute-tcp/issues).

## License

[MIT](LICENSE)
