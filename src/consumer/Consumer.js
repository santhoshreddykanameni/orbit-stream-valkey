class Consumer {
  constructor(redis, config = {}) {
    this.redis = redis;

    this.block = config.block || 100;

    this.count = config.count || 500;
  }

  async ensureGroup(stream, groupId) {
    try {
      await this.redis.xgroup(
        "CREATE",

        stream,

        groupId,

        "$",

        "MKSTREAM",
      );
    } catch (error) {
      if (!error.message.includes("BUSYGROUP")) {
        throw error;
      }
    }
  }

  async subscribe(stream, handler, options = {}) {
    const { groupId, consumerId } = options;

    await this.ensureGroup(
      stream,

      groupId,
    );

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

          messages.push({
            id,

            value: fields[1],
          });
        }
      }

      await handler(messages);

      if (ids.length) {
        await this.redis.xack(
          stream,

          groupId,

          ...ids,
        );
      }
    }
  }
}

module.exports = Consumer;
