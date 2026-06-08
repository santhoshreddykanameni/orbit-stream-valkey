# @orbit-stream/valkey

High-performance Valkey Streams transport adapter for Orbit Stream.

Optimized for:

- telemetry streaming
- satellite data pipelines
- batch ingestion
- consumer groups
- reliable processing
- ~100 Mbps sustained throughput

---

# Installation

```bash
npm install @orbit-stream/valkey
```

---

# Features

- Valkey Streams support
- Consumer Groups
- Batch publishing
- Batch consumption
- Reliable acknowledgements
- Buffer payload support
- Low operational overhead
- Telemetry optimized

---

# Usage

```js
const { OrbitStream } = require("@orbit-stream/valkey");

const stream = new OrbitStream({
  host: "127.0.0.1",

  port: 6379,

  batchSize: 500,

  flushInterval: 20,

  count: 500,

  block: 100,

  maxLen: 100000,
});

await stream.connect();
```

---

# Publish

```js
await stream.publish("telemetry", {
  value: Buffer.from("hello"),
});
```

---

# Subscribe

```js
await stream.subscribe(
  "telemetry",

  async (messages) => {
    console.log(messages.length);
  },

  {
    groupId: "telemetry-group",

    consumerId: "consumer-1",
  },
);
```

---

# Recommended Settings

```js
{
  batchSize: 500,
  flushInterval: 20,

  count: 500,
  block: 100,

  maxLen: 100000
}
```

---

# Architecture

```txt
Telemetry Source
        │
        ▼
Orbit Stream Producer
        │
        ▼
Valkey Streams
        │
        ▼
Consumer Group
        │
        ▼
Telemetry Processors
```

---

# License

MIT
