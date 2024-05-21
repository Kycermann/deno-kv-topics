export type QueueKey = string | (string | number | boolean)[];
export type QueueListener<T> = (payload: T) => void | Promise<void>;

type WrappedPayload<T> = {
  queueKey: QueueKey;
  payload: T;
  keysIfUndelivered: Deno.KvKey[];
  __mieszko_topics__: 1 | undefined;
};

// deno-lint-ignore no-explicit-any
const listeners: Map<string, QueueListener<any>> = new Map();

function computeQueueId(queueKey: QueueKey) {
  return JSON.stringify(queueKey);
}

/**
 * Enqueues a payload to the Deno KV queue. You must call `listenQueue` before calling this function.
 */
export async function enqueue<T = unknown>(
  queueKey: QueueKey,
  payload: T,
  options: Parameters<Deno.Kv["enqueue"]>[1] = {},
  atomic?: Deno.AtomicOperation,
) {
  const queueId = computeQueueId(queueKey);

  if (!listeners.has(queueId)) {
    throw new Error(`No listener for queue ${queueId}`);
  }

  const wrappedPayload: WrappedPayload<T> = {
    queueKey,
    payload,
    keysIfUndelivered: options.keysIfUndelivered ?? [],
    __mieszko_topics__: 1,
  };

  if (atomic) {
    atomic.enqueue(wrappedPayload, options);
  } else {
    await using kv = await Deno.openKv();
    const { ok } = await kv.enqueue(wrappedPayload, options);

    if (!ok) {
      console.log("Payload", wrappedPayload);
      throw new Error(`Failed to enqueue ${queueId}`);
    }
  }
}

/**
 * Listens to a queue and calls the callback when a payload is enqueued.
 */
export function listenQueue<T>(queueKey: QueueKey, callback: QueueListener<T>) {
  const queueId = computeQueueId(queueKey);

  listeners.set(queueId, callback);
}

Deno.openKv().then((kv) => {
  kv.listenQueue(
    async (
      { queueKey, payload, __mieszko_topics__ }: WrappedPayload<unknown>,
    ) => {
      if (__mieszko_topics__ !== 1) {
        throw new Error("Unknown queue payload received");
      }

      const queueId = computeQueueId(queueKey);
      const listener = listeners.get(queueId);

      if (listener) {
        await listener(payload);
      } else {
        throw new Error(`No listener for queue ${queueId}`);
      }
    },
  );
});
