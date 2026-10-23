'use strict';

const net = require('net');
const { promisify } = require('util');
const Session = require('./session');
const SessionHub = require('./session_hub');
const TcpChannel = require('./channel');

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

  async init() {
    if (this.server) return;

    this.sessionHub = new SessionHub(this.config, this.ctx);
    const server = net.createServer();
    this.server = server;

    [
      'getConnections',
      'close',
    ].forEach(method => {
      server[method] = promisify(server[method]);
    });

    server.on('connection', this.connection.bind(this));

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

    this.ctx.logger.info(`[egg-cute-tcp] tcp server listen on ${this.config.port}`);
  }

  // 设备连接服务器会将new一个channel与session, 鉴权成功拿到imei后再将session注册到sessionManager
  async connection(socket) {
    try {
      if (!this.ctx.app || !this.ctx.app.snowflake) {
        this.ctx.logger.error('[egg-cute-tcp] create server, ctx.app.snowflake is null, please check if you have installed egg-cute-snowflake plugin');
        socket.destroy();
        return;
      }
      socket.setKeepAlive(true, this.config.keepAlive);

      const noop = () => {};
      socket.on('error', noop);

      // 解决竞态问题, 防止上线后马上断线, 若提前new了session与channel, await期间连接断开:channel已经close,随后创建的session绑定到一个死channel,永远收不到close
      const sessionId = await this.ctx.app.snowflake.nextId();
      // await 期间连接已经断开, 不再创建 channel/session
      if (socket.destroyed) return;

      socket.off('error', noop);

      // 一个channel对应一个session, 表示一台设备连接到了服务器
      // 因为channel构造函数中注册了socket事件, 所以New的这个channel会存在内存中
      const channel = new TcpChannel(socket, this.config, this.ctx);
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
