'use strict';
const assert = require('assert');
const rabbitSubscribe = Symbol('cuteTcp#rabbitSubscribe');

class PubSubRabbit {
  constructor(app, config) {
    this.app = app;
    this.config = config;
    this.exchange = config.pubSubChannel;
    this.ch = null;
    this.onMessage = null;
    this.consumerTag = null;
  }

  getChannel() {
    const r = this.config.pubSubClient ? this.app.rabbit.get(this.config.pubSubClient) : this.app.rabbit;
    this.ch = r.ch || r; // v2: { conn, ch }; v1: 直接是 channel
  }

  async start(onMessage) {
    assert(this.app.rabbit, '[egg-cute-tcp] pubSubType is rabbit, but app.rabbit is not found, please enable the rabbit plugin');
    this.onMessage = onMessage;
    this.getChannel();
    await this[rabbitSubscribe]();
  }

  async cancel() {
    if (!this.consumerTag) return;
    const tag = this.consumerTag;
    this.consumerTag = null;
    try { await this.ch.cancel(tag); } catch (_) {}   // channel 已坏时会抛, 忽略
  }

  // egg-rabbitmq 库重连成功后调用: app.tcpPubSub && app.tcpPubSub.restart()
  async restart() {
    if (!this.onMessage) return;
    await this.cancel();        // 旧 channel 仍然存活时避免重复消费
    this.getChannel();
    await this[rabbitSubscribe]();
  }

  async [rabbitSubscribe]() {
    // fanout + 每个 worker 一个独占临时队列 = 广播给所有 worker
    await this.ch.assertExchange(this.exchange, 'fanout', { durable: false });
    const { queue } = await this.ch.assertQueue('', { exclusive: true, autoDelete: true });
    await this.ch.bindQueue(queue, this.exchange, '');
    const ok = await this.ch.consume(queue, msg => {
      if (msg && this.onMessage) this.onMessage(msg.content);
    }, { noAck: true });
    this.consumerTag = ok.consumerTag;
  }

  async publish(payload) {
    this.ch.publish(this.exchange, '', Buffer.from(JSON.stringify(payload)));
  }

  async stop() {
    this.onMessage = null; // 之后即使被调用 restart 也不会再订阅
    await this.cancel();
  }
}

module.exports = PubSubRabbit;