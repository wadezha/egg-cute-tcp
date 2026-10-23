'use strict';

class PubSubRedis {
  constructor(app, config) {
    this.app = app;
    this.config = config;
    this.pubClient = null;
    this.subClient = null;
  }

  async start(onMessage) {
    this.pubClient = this.config.pubSubClient ? this.app.redis.get(this.config.pubSubClient) : this.app.redis;
    this.subClient = this.pubClient.duplicate();
    this.subClient.on('error', err => this.app.coreLogger.error('[egg-cute-tcp] redis sub error', err));
    this.subClient.on('message', (channel, raw) => {
      if (channel === this.config.pubSubChannel) onMessage(raw);
    });
    await this.subClient.subscribe(this.config.pubSubChannel);
  }

  async publish(payload) {
    await this.pubClient.publish(this.config.pubSubChannel, JSON.stringify(payload));
  }

  async stop() {
    if (!this.subClient) return;
    await this.subClient.unsubscribe(this.config.pubSubChannel).catch(() => {});
    this.subClient.disconnect();
  }
}

module.exports = PubSubRedis;