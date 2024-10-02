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
export type TopicQueueConnection = {
  enqueue: <T = unknown>(
    topicKey: Topic,
    payload: T,
    options?: Parameters<Deno.Kv["enqueue"]>[1],
    atomic?: Deno.AtomicOperation,
  ) => Promise<void>;
  listenQueue: <T>(topicKey: Topic, callback: QueueListener<T>) => void;
  close: () => void;
  [Symbol.dispose]: () => void;
};

/**
 * The payload that is actually enqueued to Deno KV.
 */
export type WrappedPayload<T> = {
  topicKey: Topic;
  payload: T;
  __mieszko_topics__: 1 | undefined;
};
