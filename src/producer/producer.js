const { serializer } = require("@orbit-stream/core");

class Producer {
  constructor(redis, config = {}) {
    this.redis = redis;

    this.maxLen = config.maxLen || 100000;
  }

  async publish(stream, message) {
    await this.redis.xadd(
      stream,

      "MAXLEN",
      "~",
      this.maxLen,

      "*",

      "data",

      serializer.serialize(message.value),
    );
  }

  async publishBatch(stream, messages) {
    const pipeline = this.redis.pipeline();

    for (let i = 0; i < messages.length; i++) {
      pipeline.xadd(
        stream,

        "MAXLEN",
        "~",
        this.maxLen,

        "*",

        "data",

        serializer.serialize(messages[i].value),
      );
    }

    await pipeline.exec();
  }
}

module.exports = Producer;
