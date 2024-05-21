/**
 * Topics are strings or arrays of strings, numbers, or booleans.
 */
export type Topic = string | (string | number | boolean)[];

/**
 * Queue listeners are called when a payload is received from the Deno KV queue.
 */
export type QueueListener<T> = (payload: T) => void | Promise<void>;

type WrappedPayload<T> = {
  topicKey: Topic;
  payload: T;
  keysIfUndelivered: Deno.KvKey[];
  __mieszko_topics__: 1 | undefined;
};

// deno-lint-ignore no-explicit-any
const listeners: Map<string, QueueListener<any>> = new Map();

function getTopicId(topicKey: Topic) {
  return JSON.stringify(topicKey);
}

/**
 * Enqueues a payload to the Deno KV queue. You must call `listenQueue` before calling this function.
 */
export async function enqueue<T = unknown>(
  topicKey: Topic,
  payload: T,
  options: Parameters<Deno.Kv["enqueue"]>[1] = {},
  atomic?: Deno.AtomicOperation,
) {
  const queueId = getTopicId(topicKey);

  if (!listeners.has(queueId)) {
    throw new Error(`No listener for queue ${queueId}`);
  }

  const wrappedPayload: WrappedPayload<T> = {
    topicKey,
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
export function listenQueue<T>(topicKey: Topic, callback: QueueListener<T>) {
  const queueId = getTopicId(topicKey);

  listeners.set(queueId, callback);
}

Deno.openKv().then((kv) => {
  kv.listenQueue(
    async (
      { topicKey, payload, __mieszko_topics__ }: WrappedPayload<unknown>,
    ) => {
      if (__mieszko_topics__ !== 1) {
        throw new Error("Unknown queue payload received");
      }

      const queueId = getTopicId(topicKey);
      const listener = listeners.get(queueId);

      if (listener) {
        await listener(payload);
      } else {
        throw new Error(`No listener for queue ${queueId}`);
      }
    },
  );
});
