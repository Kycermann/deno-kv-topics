import type {
  QueueListener,
  Topic,
  TopicQueueConnection,
  WrappedPayload,
} from "./types.ts";

export type { QueueListener, Topic, TopicQueueConnection };

function getTopicId(topicKey: Topic) {
  return JSON.stringify(topicKey);
}

/**
 * Connects to Deno KV and returns functions to enqueue and listen to a queue with topics.
 *
 * @param kvInstance Deno KV connection to use. If not provided, a new connection will be created.
 * @param disableListenQueue Whether to disable listening to the KV queue. Default is `false`.
 */
export async function connectTopicQueue(
  kvInstance?: Deno.Kv,
  disableListenQueue = false,
): Promise<TopicQueueConnection> {
  const kv = kvInstance ?? (await Deno.openKv());
  const listeners: Map<string, QueueListener<unknown>> = new Map();

  /**
   * Enqueues a payload to the Deno KV queue. You must call `listenQueue` before calling this function.
   *
   * @param topicKey The topic to enqueue the payload to.
   * @param payload The payload to enqueue.
   * @param options Options to pass to `Deno.Kv.enqueue`.
   * @param atomic Atomic operation to use for enqueuing the payload.
   */
  async function enqueue<T = unknown>(
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
      __mieszko_topics__: 1,
    };

    if (atomic) {
      atomic.enqueue(wrappedPayload, options);
    } else {
      const { ok } = await kv.enqueue(wrappedPayload, options);

      if (!ok) {
        throw new Error(`Failed to enqueue into ${queueId}`);
      }
    }
  }

  /**
   * Listens to a queue and calls the callback when a payload is enqueued.
   *
   * @param topicKey The topic to listen to.
   * @param callback The callback to call when a payload is enqueued.
   */
  function listenQueue<T>(topicKey: Topic, callback: QueueListener<T>) {
    const queueId = getTopicId(topicKey);

    listeners.set(queueId, callback as QueueListener<unknown>);

    if (listeners.size > 1 || disableListenQueue) {
      return;
    }

    kv.listenQueue(
      async ({
        topicKey,
        payload,
        __mieszko_topics__,
      }: WrappedPayload<unknown>) => {
        if (__mieszko_topics__ !== 1) {
          throw new Error("Unrecognised payload format");
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
  }

  /**
   * Closes the underlying Deno KV connection.
   */
  function close() {
    kv.close();
  }

  return {
    enqueue,
    listenQueue,
    close,
    [Symbol.dispose]: close,
  };
}
