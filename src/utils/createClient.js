const Redis = require("ioredis");

function createClient(config = {}) {
  return new Redis({
    host: config.host || "127.0.0.1",

    port: config.port || 6379,

    username: config.username,

    password: config.password,

    lazyConnect: true,

    maxRetriesPerRequest: null,

    enableReadyCheck: false,

    keepAlive: 30000,

    connectTimeout: 30000,

    retryStrategy(times) {
      return Math.min(times * 100, 3000);
    },
  });
}

module.exports = createClient;
