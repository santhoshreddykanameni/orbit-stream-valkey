const { serializer } = require("@orbit-stream/core");

class Consumer {
  constructor(redis, config = {}) {
    this.redis = redis;

    this.connected = false;

    this.block = config.block || 100;

    this.count = config.count || 500;
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

  async ensureGroup(stream, groupId, fromBeginning = false) {
    try {
      await this.redis.xgroup(
        "CREATE",
        stream,
        groupId,
        fromBeginning ? "0" : "$",
        "MKSTREAM",
      );
    } catch (error) {
      if (!error.message.includes("BUSYGROUP")) {
        throw error;
      }
    }
  }

  deserializeValue(value) {
    if (!value) {
      return null;
    }

    try {
      return serializer.deserialize(value);
    } catch {
      return value;
    }
  }

  deserializeHeaders(headers) {
    if (!headers) {
      return {};
    }

    try {
      return JSON.parse(headers);
    } catch {
      return {};
    }
  }

  async subscribe(stream, handler, options = {}) {
    const groupId =
      options.groupId || options.consumerGroup || "orbit-stream-group";

    const consumerId = options.consumerId || options.clientId || "consumer-1";

    await this.ensureGroup(stream, groupId, options.fromBeginning || false);

    while (true) {
      const result = await this.redis.xreadgroup(
        "GROUP",
        groupId,
        consumerId,
        "COUNT",
        this.count,
        "BLOCK",
        this.block,
        "STREAMS",
        stream,
        ">",
      );

      if (!result) {
        continue;
      }

      const ids = [];

      const messages = [];

      for (const [, entries] of result) {
        for (const [id, fields] of entries) {
          ids.push(id);

          const map = {};

          for (let i = 0; i < fields.length; i += 2) {
            map[fields[i]] = fields[i + 1];
          }

          const timestamp = Number(id.split("-")[0]);

          messages.push({
            key: map.key || null,

            value: this.deserializeValue(map.data),

            headers: this.deserializeHeaders(map.headers),

            timestamp,

            partition: 0,

            offset: id,

            id,
          });
        }
      }

      try {
        await handler(messages);

        if (options.autoCommit ?? true) {
          await this.redis.xack(stream, groupId, ...ids);
        }
      } catch (error) {
        console.error("[OrbitStream] Consumer handler failed:", error);
      }
    }
  }
}

module.exports = Consumer;
