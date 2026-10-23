'use strict';

const assert = require('assert');

const tcp = require('./lib/tcp');
const PubSubRedis = require('./lib/pubsub/redis');
const PubSubRabbit = require('./lib/pubsub/rabbit');

module.exports = app => {
  if (app.config.tcp.client) app.config.tcp.client.protocol = app.config.tcp.client.protocol || 'ect';
  if (app.config.tcp.clients) app.config.tcp.clients = Object.fromEntries(Object.entries(app.config.tcp.clients).map(([k, v]) => [k, { ...v, protocol: k }]));

  const config = app.config.tcp;
  if (config.app) {
    const clients = config.client ? [config.client] : Object.values(config.clients || {});
    const sessionKeys = clients.flatMap(c => [c.sessionKeyCacheKey, c.sessionIdCacheKey]);
    assert(sessionKeys.every(Boolean) && new Set(sessionKeys).size === sessionKeys.length, '[egg-cute-tcp] sessionKeyCacheKey / sessionIdCacheKey are required and must be unique across clients');

    tcp(app);
    assert(['redis', 'rabbit'].includes(config.pubSubType), `[egg-cute-tcp] pubSubType must be 'redis' or 'rabbit'`);
    app.tcpPubSub = config.pubSubType === 'redis' ? new PubSubRedis(app, config) : new PubSubRabbit(app, config);
  }

  const EVENTS = new Map([
    ['client.connected', 'cute-tcp:client-connected'],
    [config.eventSend, 'cute-tcp:client-send'],
    [config.eventKickout, 'cute-tcp:client-kickout'],
  ]);

  const onMessage = (raw) => {
    let payload;
    try {
      payload = JSON.parse(raw.toString());
    } catch (err) {
      app.coreLogger.error('[egg-cute-tcp] parse pub message error', err);
      return;
    }
    const eventName = payload && EVENTS.get(payload.type);
    if (!eventName) return;
    try {
      app.emit(eventName, payload);
    } catch (err) {
      app.coreLogger.error('[egg-cute-tcp] handle pub message error', err);
    }
  }

  app.beforeClose(async () => {
    if (app.tcpPubSub) await app.tcpPubSub.stop();
  });

  app.ready(async () => {
    try {
      if (!config.app) return;

      // ready 在所有 beforeStart 都完成之后才触发, 这时 egg-rabbitmq 的连接一定已经建好
      if (app.tcpPubSub) {
        await app.tcpPubSub.start(onMessage);
        app.coreLogger.info(`[egg-cute-tcp] subscribe ok, type: ${config.pubSubType}, pid: ${process.pid}`);
      }

      if (config.client) {
        await app.tcp.init();
        return;
      }
      if (!config.clients) return;
      for (const k of Object.keys(config.clients)) {
        await app.tcp.get(k).init();
      }
    } catch (err) {
      app.coreLogger.error('[egg-cute-tcp] init failed, exit', err);
      process.exit(1);
    }
  });
};
