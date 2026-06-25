const { serializer } = require("@orbit-stream/core");

class Producer {
  constructor(redis, config = {}) {
    this.redis = redis;

    this.connected = false;

    this.maxLen = config.maxLen || 100000;

    /*
     * Keep Valkey pipeline sizes bounded,
     * similar to Redpanda's maxBatchBytes.
     */
    this.maxBatchBytes = config.maxBatchBytes || 8 * 1024 * 1024;
  }

  async connect() {
    if (this.connected) {
      return;
    }

    this.connected = true;
  }

  async disconnect() {
    if (!this.connected) {
      return;
    }

    this.connected = false;
  }

  serializeValue(value) {
    if (value == null) {
      return null;
    }

    if (Buffer.isBuffer(value)) {
      return value;
    }

    return serializer.serialize(value);
  }

  serializeKey(key) {
    if (key == null) {
      return "";
    }

    return Buffer.isBuffer(key) ? key.toString() : String(key);
  }

  serializeHeaders(headers = {}) {
    const result = {};

    for (const [key, value] of Object.entries(headers)) {
      if (value == null) {
        continue;
      }

      result[key] = Buffer.isBuffer(value)
        ? value.toString("base64")
        : String(value);
    }

    return result;
  }

  estimateMessageSize(message) {
    let size = 0;

    if (message.key) {
      size += Buffer.byteLength(String(message.key));
    }

    if (message.value) {
      if (Buffer.isBuffer(message.value)) {
        size += message.value.length;
      } else {
        size += Buffer.byteLength(JSON.stringify(message.value));
      }
    }

    if (message.headers) {
      for (const [key, value] of Object.entries(message.headers)) {
        size += Buffer.byteLength(key);

        if (Buffer.isBuffer(value)) {
          size += value.length;
        } else {
          size += Buffer.byteLength(String(value));
        }
      }
    }

    return size;
  }

  async sendBatch(stream, messages) {
    const pipeline = this.redis.pipeline();

    for (const msg of messages) {
      pipeline.xadd(
        stream,

        "MAXLEN",
        "~",
        this.maxLen,

        "*",

        "key",
        this.serializeKey(msg.key),

        "headers",
        JSON.stringify(this.serializeHeaders(msg.headers)),

        "data",
        this.serializeValue(msg.value),
      );
    }

    await pipeline.exec();
  }

  async publish(stream, message) {
    return this.publishBatch(stream, [message]);
  }

  async publishBatch(stream, messages) {
    if (!messages?.length) {
      return;
    }

    let batch = [];

    let batchBytes = 0;

    for (const message of messages) {
      const messageSize = this.estimateMessageSize(message);

      /*
       * Keep pipeline size under maxBatchBytes.
       */
      if (batch.length > 0 && batchBytes + messageSize > this.maxBatchBytes) {
        await this.sendBatch(stream, batch);

        batch = [];

        batchBytes = 0;
      }

      batch.push(message);

      batchBytes += messageSize;
    }

    if (batch.length > 0) {
      await this.sendBatch(stream, batch);
    }
  }
}

module.exports = Producer;
