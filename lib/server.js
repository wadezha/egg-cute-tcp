'use strict';

const net = require('net');
const tls = require('tls');
const { promisify } = require('util');
const Session = require('./session');
const SessionHub = require('./session_hub');
const TcpChannel = require('./channel');
const Utils = require('./utils');

class TcpServer {
  constructor (config, ctx) {
    this.inHandlers = [];
    this.outHandlers = [];

    config.idleTimeout = (config.idleTimeout || 600) * 1000;
    this.config = config;
    this.ctx = ctx;

    this.server = null;
    this.parser = {};
    this.chain = {};

    this.sessionHub = null;
  }

  useInHandler(fn) {
    this.inHandlers.push(fn);
  }

  useOutHandler(fn) {
    this.outHandlers.push(fn);
  }

  // tls.enable = false: plain TCP, the 'connection' event
  // tls.enable = true: the other tls options go straight to tls.createServer(), the 'secureConnection' event
  // fires after the handshake, so only devices that completed it get a channel / session
  createServer() {
    const { enable, ...options } = this.config.tls || {};
    if (!enable) {
      const server = net.createServer();
      server.on('connection', this.connection.bind(this));
      return server;
    }
    const utils = new Utils();
 
    for (const k of ['key', 'cert', 'ca', 'pfx']) {
      if (options[k]) options[k] = utils.readPem(options[k], this.ctx.app.config.baseDir);
    }
    const server = tls.createServer(options);
    server.on('secureConnection', this.connection.bind(this));
    // handshake failures (plain TCP to a TLS port, scanners, bad certificates ...)
    server.on('tlsClientError', (err, socket) => {
      this.ctx.logger.warn(`[egg-cute-tcp] tls handshake failed, remote: ${socket && socket.remoteAddress}, ${err.message}`);
      if (socket) socket.destroy();
    });
    return server;
  }

  async init() {
    if (this.server) return;

    const snowflake = this.ctx.app.snowflake;
    if (!snowflake || typeof snowflake.nextId !== 'function') {
      throw new Error('[egg-cute-tcp] app.snowflake.nextId() not found, please enable the egg-cute-snowflake plugin');
    }

    // create first: unreadable certificate files throw here, before the hub registered anything
    const server = this.createServer();
    this.sessionHub = new SessionHub(this.config, this.ctx);
    this.server = server;

    [
      'getConnections',
      'close',
    ].forEach(method => {
      server[method] = promisify(server[method]);
    });

    // 常驻的 error 监听, 监听成功后的 server 错误只记日志, 不会崩进程
    server.on('error', err => {
      this.ctx.logger.error('[egg-cute-tcp] tcp server error', err);
    });

    try {
      await new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(this.config.port, () => {
          server.off('error', reject);
          resolve();
        });
      });
    } catch (err) {
      this.server = null;
      this.sessionHub.destroy();
      this.sessionHub = null;
      throw err;
    }

    this.ctx.logger.info(`[egg-cute-tcp] tcp server listen on ${this.config.port}${this.config.tls && this.config.tls.enable ? ' (tls)' : ''}`);
  }

  // 设备连接服务器会将new一个channel与session, 鉴权成功拿到imei后再将session注册到sessionManager
  async connection(socket) {
    try {
      // 在 channel 接管 socket 之前, 下面的 await 期间设备如果 RST, 没有 error 监听就是 uncaught exception
      const noop = () => {};
      socket.on('error', noop);

      socket.setKeepAlive(true, this.config.keepAlive);

      // 解决竞态问题, 防止上线后马上断线, 若提前new了session与channel, await期间连接断开:channel已经close,随后创建的session绑定到一个死channel,永远收不到close
      const sessionId = await this.ctx.app.snowflake.nextId();
      // await 期间连接已经断开, 不再创建 channel/session
      if (socket.destroyed) return;

      // 一个channel对应一个session, 表示一台设备连接到了服务器
      // 因为channel构造函数中注册了socket事件, 所以New的这个channel会存在内存中
      const channel = new TcpChannel(socket, this.config, this.ctx);
      // channel 已经挂上了自己的 error 监听
      socket.off('error', noop);
      channel.sessionHub = this.sessionHub;

      this.inHandlers.forEach((inHandler) => {
        channel.addInHandler(inHandler);
      });

      this.outHandlers.forEach((outHandler) => {
        channel.addOutHandler(outHandler);
      });

      // 因为session与channel做了双向绑定, New一个对象时会将这个session绑定到channel中, 所以New的这个session也会存在内存中
      const session = new Session(channel);
      session.sessionId = sessionId;
      session.sessionKey = '';
      session.clientIp = socket.remoteAddress;
      session.clientPort = socket.remotePort;
      session.connectTime = new Date().toISOString().replace('T', ' ').slice(0, 19);
      channel.logger('info', 'connect');

      await this.sessionHub.addSession(session);
    } catch (err) {
      this.ctx.logger.error('[egg-cute-tcp] connection init error', err);
      socket.destroy(); // channel 通过 close 监听自行清理
    }
  }

  // 只会停止接受新连接, 已经存在的 socket 全部保持连接, 不会自动销毁
  // 若要销毁需通知 SessionManager 踢掉现有设备
  async close() {
    if (!this.server) return;
    const server = this.server;
    this.server = null;
    await server.close();
  }

  async destroy() {
    if (this.sessionHub) this.sessionHub.destroy();
  }
}

module.exports = TcpServer;
