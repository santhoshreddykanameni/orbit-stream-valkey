const {
  BaseAdapter,

  BatchProcessor,
} = require("@orbit-stream/core");

const createClient = require("../utils/createClient");

const Producer = require("../producer/producer");

const Consumer = require("../consumer/Consumer");

class ValkeyAdapter extends BaseAdapter {
  constructor(config = {}) {
    super(config);

    this.redis = createClient(config);

    this.producer = new Producer(this.redis, config);

    this.consumer = new Consumer(this.redis, config);

    this.batchProcessor = new BatchProcessor({
      batchSize: config.batchSize || 500,

      flushInterval: config.flushInterval || 20,
    });
  }

  async connect() {
    await this.redis.connect();

    this.emitConnected();
  }

  async disconnect() {
    await this.redis.quit();

    this.emitDisconnected();
  }

  async publish(stream, message) {
    this.batchProcessor.add(
      message,

      async (batch) => {
        await this.publishBatch(
          stream,

          batch,
        );
      },
    );
  }

  async publishBatch(stream, messages) {
    return this.producer.publishBatch(
      stream,

      messages,
    );
  }

  async subscribe(stream, handler, options = {}) {
    return this.consumer.subscribe(
      stream,

      handler,

      options,
    );
  }
}

module.exports = ValkeyAdapter;
