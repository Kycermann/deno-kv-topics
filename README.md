# Topics for Deno KV Queues

[![Publish](https://github.com/Kycermann/deno-kv-topics/actions/workflows/publish.yml/badge.svg)](https://github.com/Kycermann/deno-kv-topics/actions/workflows/publish.yml)
[![deno module](https://shield.deno.dev/x/topics)](https://deno.land/x/topics)
[![JSR](https://jsr.io/badges/@mieszko/topics)](https://jsr.io/@mieszko/topics)

Use Deno KV queues with topics.

This module calls `kv.listenQueue` internally, so you cannot call
`kv.listenQueue` yourself or use any modules that call `kv.listenQueue`. It is
not recommended to use `kv.enqueue` directly because this module will ignore
those items.

## Quick Start

```typescript
import { connectTopicQueue } from "jsr:@mieszko/topics";

const { enqueue, listenQueue } = await connectTopicQueue();

// Call listenQueue on the top level.
listenQueue("my-queue", async (data) => {
  console.log(data);
});

enqueue("my-queue", { example: "Hello!" });
```

## Advanced Usage

```typescript
import { connectTopicQueue } from "jsr:@mieszko/topics";

// Connect to any KV store by passing a KV connection to connectTopicQueue.
const kv = await Deno.openKv(":memory:");
const { enqueue, listenQueue, close } = await connectTopicQueue(kv);

type MyData = {
  example: string;
};

listenQueue<MyData>(["my", "queue"], async (data) => {
  console.log(data.example);
});

// You can optionally pass an atomic instance to enqueue.
const atomic = kv.atomic();

// If you call enqueue before listenQueue, an error will be thrown.
enqueue<MyData>(
  ["my", "queue"],
  { example: "Hello, World!" },
  { backOffSchedule: [5000, 10_000] }, // Optional
  atomic, // Optional
);

await atomic.commit();

// Close the connection when you are done.
close();
```
