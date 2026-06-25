const { OrbitStream } = require("../index");

async function main() {
  const stream = new OrbitStream({
    clientId: "consumer-1",
    groupId: "telemetry-group",
    brokers: ["localhost:6379"],
    password: "orbit-stream",
    manageTopics: true,

    batchSize: 100,
  });

  await stream.connect();

  await stream.ensureTopic({
    name: "telemetry-stream",
  });

  console.log("Subscriber connected");

  await stream.subscribe(
    "telemetry-stream",

    async (messages) => {
      console.log(`Received ${messages.length} messages`);

      for (const message of messages) {
        console.log({
          id: message.id,
          key: message.key,
          timestamp: message.timestamp,
          value: message.value,
          headers: message.headers,
        });
      }
    },

    {
      fromBeginning: true,

      autoCommit: true,
    },
  );
}

main().catch(console.error);
