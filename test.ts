import { connectTopicQueue } from "./mod.ts";
import { assertEquals } from "jsr:@std/assert";
import type { WrappedPayload } from "./types.ts";

Deno.test("Should enqueue and receive payloads", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  const testTopic = "testTopic";
  const testPayload = { data: "testData" };

  let receivedPayload;
  const { resolve, promise: received } = Promise.withResolvers();

  // Setup listener
  q.listenQueue(testTopic, (payload) => {
    receivedPayload = payload;
    resolve(payload);
  });

  // Enqueue payload and wait for it to be received
  q.enqueue(testTopic, testPayload);
  await received;

  // Ensure payload is received as expected
  assertEquals(receivedPayload, testPayload);
});

Deno.test("Should direct payloads to the correct topic with multiple topics", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  const testTopic1 = "testTopic";
  const testTopic2 = "testTopic2";
  const testPayload1 = { data: "testData1" };
  const testPayload2 = { data: "testData2" };

  let receivedPayload1;
  let receivedPayload2;

  const { resolve: resolve1, promise: received1 } = Promise.withResolvers();
  const { resolve: resolve2, promise: received2 } = Promise.withResolvers();

  // Setup listeners
  q.listenQueue(testTopic1, (payload) => {
    receivedPayload1 = payload;
    resolve1(payload);
  });

  q.listenQueue(testTopic2, (payload) => {
    receivedPayload2 = payload;
    resolve2(payload);
  });

  // Enqueue payloads and wait for them to be received
  q.enqueue(testTopic1, testPayload1),
    q.enqueue(testTopic2, testPayload2),
    await Promise.all([received1, received2]);

  // Ensure payloads are received as expected
  assertEquals(receivedPayload1, testPayload1);
  assertEquals(receivedPayload2, testPayload2);
});

Deno.test("Should throw on enqueue when no listener is set", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  const testTopic = "testTopic2";
  const testPayload = { data: "testData" };

  let caughtError = "";

  // Ensure error is thrown
  await q.enqueue(testTopic, testPayload).catch((error) => {
    caughtError = error.toString();
  });

  assertEquals(caughtError, `Error: No listener for queue "${testTopic}"`);
});

Deno.test("Should call kv.listenQueue only once", async () => {
  await using kv = await Deno.openKv();

  // Mock kv.listenQueue to count calls
  const kvListenQueue = kv.listenQueue.bind(kv);

  let listenQueueCalls = 0;

  kv.listenQueue = async (handler: (value: unknown) => void) => {
    listenQueueCalls++;
    await kvListenQueue(handler);
  };

  await using q = await connectTopicQueue(kv);

  // Set two listeners
  q.listenQueue("testTopic", () => {});
  q.listenQueue("testTopic2", () => {});

  // Ensure kv.listenQueue is only called once
  assertEquals(listenQueueCalls, 1);
});

Deno.test("Should reject payloads without __mieszko_topics__ field", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  // Mock kv.listenQueue to catch errors
  const { resolve, promise: caught } = Promise.withResolvers();
  const kvListenQueue = kv.listenQueue.bind(kv);
  let caughtError = "";

  kv.listenQueue = async (handler: (value: unknown) => Promise<void>) => {
    await kvListenQueue(async (payload) => {
      try {
        await handler(payload);
      } catch (ex) {
        caughtError = ex.toString();
        resolve(caughtError);
      }
    });
  };

  // Start listening
  q.listenQueue("testTopic", () => {});

  // Enqueue a payload without __mieszko_topics__ field
  kv.enqueue({ thisWillBe: "a rejected payload" });
  await caught;

  // Ensure error is thrown
  assertEquals(caughtError, "Error: Unrecognised payload format");
});

Deno.test("Should reject received payloads on topics without a listener", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  // Mock kv.listenQueue to catch errors
  const { resolve, promise: caught } = Promise.withResolvers();
  const kvListenQueue = kv.listenQueue.bind(kv);
  let caughtError = "";

  kv.listenQueue = async (handler: (value: unknown) => Promise<void>) => {
    await kvListenQueue(async (payload) => {
      try {
        await handler(payload);
      } catch (ex) {
        caughtError = ex.toString();
        resolve(caughtError);
      }
    });
  };

  // Start listening
  q.listenQueue("testTopic", () => {});

  // Enqueue a payload without __mieszko_topics__ field
  await kv.enqueue({
    topicKey: "topicWithoutListener",
    payload: { thisWillBe: "a rejected payload" },
    __mieszko_topics__: 1,
  });

  await caught;

  // Ensure error is thrown
  assertEquals(
    caughtError,
    `Error: No listener for queue "topicWithoutListener"`,
  );
});

Deno.test("Should throw when trying to enqueue on a topic without a listener", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  const testTopic = "testTopic";
  const testPayload = { data: "testData" };

  let caughtError = "";

  // Ensure error is thrown
  await q.enqueue(testTopic, testPayload).catch((error) => {
    caughtError = error.toString();
  });

  assertEquals(caughtError, `Error: No listener for queue "${testTopic}"`);
});

Deno.test("Should use atomic if provided", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  const atomic = kv.atomic();

  const testTopic = "testTopic";
  const testPayload = { data: "testData" };

  const { resolve, promise: calledAtomicEnqueue } = Promise.withResolvers<
    WrappedPayload<typeof testPayload>
  >();

  atomic.enqueue = resolve as Deno.AtomicOperation["enqueue"];

  // Setup listener and enqueue
  q.listenQueue(testTopic, () => {});
  q.enqueue(testTopic, testPayload, {}, atomic);

  const { payload: enqueuedPayload } = await calledAtomicEnqueue;

  // Ensure atomic.enqueue was called
  assertEquals(enqueuedPayload, testPayload);
});

Deno.test("Should throw error if kv.enqueue fails", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  const testTopic = "testTopic";
  const testPayload = { data: "testData" };

  let caughtError = "";

  // @ts-ignore - Mock kv.enqueue to fail
  kv.enqueue = () => ({ ok: false });

  // Setup listener
  q.listenQueue(testTopic, () => {});

  // Ensure error is thrown
  await q.enqueue(testTopic, testPayload).catch((error) => {
    caughtError = error.toString();
  });

  assertEquals(caughtError, `Error: Failed to enqueue into "${testTopic}"`);
});

Deno.test("Should pass options to kv.enqueue", async () => {
  await using kv = await Deno.openKv(":memory:");
  await using q = await connectTopicQueue(kv);

  const testTopic = "testTopic";
  const testPayload = { data: "testData" };
  const testOptions = { delay: 1 };

  const { resolve, promise: optionsFromEnqueue } = Promise.withResolvers();

  // @ts-ignore - Mock to check options
  kv.enqueue = (_value: unknown, options: unknown) => {
    resolve(options);
    return { ok: true };
  };

  // Setup listener and enqueue
  q.listenQueue(testTopic, () => {});
  q.enqueue(testTopic, testPayload, testOptions);

  const options = await optionsFromEnqueue;

  // Ensure kv.enqueue was called with the correct options
  assertEquals(options, testOptions);
});
