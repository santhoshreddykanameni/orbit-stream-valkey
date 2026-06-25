const Redis = require("ioredis");

function createClient(config = {}) {
  let host = config.host || "127.0.0.1";
  let port = config.port || 6379;

  if (Array.isArray(config.brokers) && config.brokers.length > 0) {
    const [brokerHost, brokerPort] = config.brokers[0].split(":");

    host = brokerHost;
    port = Number(brokerPort || 6379);
  }

  return new Redis({
    host,

    port,

    username: config.username,

    password: config.password,

    db: config.db || 0,

    lazyConnect: true,

    enableReadyCheck: false,

    maxRetriesPerRequest: null,

    keepAlive: config.keepAlive || 30000,

    connectTimeout: config.connectionTimeout || 30000,

    retryStrategy(times) {
      const retries = config.retry?.retries ?? 10;

      if (times > retries) {
        return null;
      }

      return Math.min(times * 100, 3000);
    },
  });
}

module.exports = createClient;
