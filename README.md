# Topics for Deno KV Queues

Use Deno KV queues with topics.

This module calls `kv.listenQueue` internally, so you cannot call
`kv.listenQueue` yourself or use any modules that call `kv.listenQueue`. It is
not recommended to use `kv.enqueue` directly because this module will ignore
those items.

## Quick Start

```typescript
import { enqueue, listenQueue } from "https://deno.land/x/topics/mod.ts";

// Call listenQueue on the top level.
listenQueue("my-queue", async (data) => {
  console.log(data);
});

enqueue("my-queue", { example: "Hello!" });
```

## Advanced Usage

```typescript
import { enqueue, listenQueue } from "https://deno.land/x/topics/mod.ts";

type MyData = {
  example: string;
};

listenQueue<MyData>(["my", "queue"], async (data) => {
  console.log(data.example);
});

// You can optionally pass an atomic instance to enqueue.
await using kv = await Deno.openKv();
const atomic = kv.atomic();

// If you call enqueue before listenQueue, an error will be thrown.
enqueue<MyData>(
  ["my", "queue"],
  { example: "Hello, World!" },
  { backOffSchedule: [5000, 10_000] }, // Optional
  atomic, // Optional
);

await atomic.commit();
```
