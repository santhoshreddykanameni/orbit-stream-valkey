const { OrbitStream } = require("../index");

async function main() {
  const stream = new OrbitStream({
    clientId: "publisher",
    brokers: ["localhost:6379"],
    password: "orbit-stream",
    manageTopics: true,
    batchSize: 100,
    maxBatchBytes: 4 * 1024 * 1024,
    flushInterval: 100,
  });

  await stream.connect();

  await stream.ensureTopic({
    name: "telemetry-stream",
  });

  console.log("Publisher connected");

  let sequence = 0;

  setInterval(async () => {
    const messages = [];

    for (let i = 0; i < 1; i++) {
      sequence++;

      messages.push({
        key: "TM",

        value: {
          sequence,
          satelliteId: "SAT-001",
          timestamp: Date.now(),
          data: Buffer.alloc(1024),
        },

        headers: {
          source: "GroundStation-1",
        },
      });
    }

    await stream.publishBatch("telemetry-stream", messages);

    console.log(`Published ${messages.length} messages`);
  }, 1000);
}

main().catch(console.error);
