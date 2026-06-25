const { BaseAdapter, BatchProcessor } = require("@orbit-stream/core");

const createClient = require("../utils/createClient");
const Producer = require("../producer/Producer");
const Consumer = require("../consumer/Consumer");

class ValkeyAdapter extends BaseAdapter {
  constructor(config = {}) {
    super(config);

    this.config = config;

    this.client = createClient(config);

    this.manageTopics = config.manageTopics === true;

    this.producer = new Producer(this.client, config);

    this.consumer = new Consumer(this.client, config);

    this.batchProcessor = new BatchProcessor({
      batchSize: config.batchSize || 5000,
      maxBatchBytes: config.maxBatchBytes || 4 * 1024 * 1024,
      flushInterval: config.flushInterval || 100,
    });

    this.publishCallback = async (stream, batch) => {
      await this.publishBatch(stream, batch);
    };
  }

  /**
   * Creates the stream (if it doesn't exist) and
   * creates the consumer group (if it doesn't exist).
   */
  async ensureTopic(topicConfig) {
    const {
      name,
      consumerGroup = this.config.groupId || "orbit-stream-group",
    } = topicConfig;

    try {
      await this.client.xgroup("CREATE", name, consumerGroup, "$", "MKSTREAM");

      console.log(
        `[OrbitStream] Created stream '${name}' with consumer group '${consumerGroup}'`,
      );
    } catch (err) {
      if (
        err.message.includes("BUSYGROUP") ||
        err.message.includes("already exists")
      ) {
        return;
      }

      throw err;
    }
  }

  /**
   * Called during connect() if manageTopics=true
   */
  async setupTopics() {
    if (!Array.isArray(this.config.topics)) {
      return;
    }

    for (const topic of this.config.topics) {
      await this.ensureTopic(topic);
    }
  }

  async connect() {
    await this.client.connect();

    try {
      if (this.manageTopics) {
        await this.setupTopics();
      }

      await Promise.all([this.producer.connect(), this.consumer.connect()]);

      this.emitConnected();
    } catch (err) {
      await this.client.quit().catch(() => {});
      throw err;
    }
  }

  async disconnect() {
    await Promise.allSettled([
      this.producer.disconnect(),
      this.consumer.disconnect(),
    ]);

    await this.client.quit().catch(() => {});

    this.emitDisconnected();
  }

  async publish(stream, message) {
    this.batchProcessor.add(message, (batch) =>
      this.publishCallback(stream, batch),
    );
  }

  async publishBatch(stream, messages) {
    if (!messages?.length) {
      return;
    }

    return this.producer.publishBatch(stream, messages);
  }

  async subscribe(stream, handler, options = {}) {
    // Automatically create the stream/group
    // when subscribing if manageTopics=true.
    if (this.manageTopics) {
      await this.ensureTopic({
        name: stream,
      });
    }

    return this.consumer.subscribe(stream, handler, options);
  }
}

module.exports = ValkeyAdapter;
