/**
 * Topics are strings or arrays of strings, numbers, or booleans.
 */
export type Topic = string | (string | number | boolean)[];

/**
 * Queue listeners are called when a payload is received from the Deno KV queue.
 */
export type QueueListener<T> = (payload: T) => void | Promise<void>;

/**
 * Contains the functions to enqueue and listen to a Deno KV queue with topics.
 */
type TopicQueueConnection = {
  enqueue: <T = unknown>(
    topicKey: Topic,
    payload: T,
    options?: Parameters<Deno.Kv["enqueue"]>[1],
    atomic?: Deno.AtomicOperation,
  ) => Promise<void>;
  listenQueue: <T>(
    topicKey: Topic,
    callback: QueueListener<T>,
  ) => Promise<void>;
  close: () => void;
};

/**
 * The payload that is actually enqueued to Deno KV.
 */
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
 * Connects to Deno KV and returns functions to enqueue and listen to a queue with topics.
 *
 * @param kvInstance Deno KV connection to use. If not provided, a new connection will be created.
 */
export async function connectTopicQueue(
  kvInstance?: Deno.Kv,
): Promise<TopicQueueConnection> {
  const kv = kvInstance ?? await Deno.openKv();

  let listening = false;

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
      keysIfUndelivered: options.keysIfUndelivered ?? [],
      __mieszko_topics__: 1,
    };

    if (atomic) {
      atomic.enqueue(wrappedPayload, options);
    } else {
      const { ok } = await kv.enqueue(wrappedPayload, options);

      if (!ok) {
        console.log("Payload", wrappedPayload);
        throw new Error(`Failed to enqueue ${queueId}`);
      }
    }
  }

  /**
   * Listens to a queue and calls the callback when a payload is enqueued.
   *
   * @param topicKey The topic to listen to.
   * @param callback The callback to call when a payload is enqueued.
   */
  async function listenQueue<T>(topicKey: Topic, callback: QueueListener<T>) {
    const queueId = getTopicId(topicKey);

    listeners.set(queueId, callback);

    if (listening) {
      return;
    }

    listening = true;

    await kv.listenQueue(
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
  }

  /**
   * Closes the underlying Deno KV connection.
   */
  function close() {
    kv.close();
  }

  return { enqueue, listenQueue, close };
}
