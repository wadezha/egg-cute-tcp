
const events = require('events');
const idleTimer = Symbol('cuteTcp#idleTimer');
const resetIdleTimer = Symbol('cuteTcp#resetIdleTimer');
const runInChain = Symbol('cuteTcp#runInChain');
const runOutChain = Symbol('cuteTcp#runOutChain');
const dispatchQueue = Symbol('cuteTcp#dispatchQueue');

class TcpChannel extends events.EventEmitter {
  constructor(socket, config, ctx) {
    super();
    this.closed = false;
    this.session = null;
    this.sessionHub = null;

    this.inHandlers = [];
    this.outHandlers = [];

    this.socket = socket;
    this.config = config;
    this.ctx = ctx;
    this[idleTimer] = null;
    // 处理粘包分包的缓存, 放在channel比放在handler更合理, 容易回收
    // 收到先后A B数据, 在处理A数据的粘包分包时出现await, 程序会马上执行B数据的粘包分包, 如此会导致frameBuf被B数据提前篡改
    // 所以要避免粘包与分包时加入await, 也就是粘包分包的处理必须是同步的, 不能有异步操作
    this.frameBuf = null;
    this[dispatchQueue] = Promise.resolve();

    this._onData = this.dispatch.bind(this);
    this._onError = this.close.bind(this, true, 'error');
    this._onClose = this.close.bind(this, true, 'close');
    this._onEnd = this.close.bind(this, true, 'end');

    this.socket.on('data', this._onData);
    this.socket.on('error', this._onError);
    this.socket.on('close', this._onClose);
    this.socket.on('end', this._onEnd);

    // 启动空闲超时计时器
    this[resetIdleTimer](this.config.idleTimeout);
  }

  logger(level, event, ...data) {
    this.ctx.url = `/tcp/${event}`;

    const format = {
      connect: `, connected`,
      dispatch: `, message: %s`,
      error: `, error: %s`,
      timeout: `, idle time out`,
      send: `, message: %s`,
      close: `, close`,
      end: `, end`,
    };

    const id = this.session ? `${this.session.sessionId} ${this.session.sessionKey || ''}` : '';
    this.ctx.logger[level](`[egg-cute-tcp] ${event} sessionId: %s ${format[event] || ''}`, id, ...data);
  }

  // 等价原 timers._unrefActive
  // 仅收到设备上行数据调用；服务端send下发不会调用，下发不重置超时
  [resetIdleTimer](timeout) {
    if (this[idleTimer]) {
      clearTimeout(this[idleTimer]);
      this[idleTimer] = null;
    }
    if (timeout <= 0) return;
    this[idleTimer] = setTimeout(() => {
      this.logger('warn', 'timeout');
      this.close(true);
    }, timeout);
    // 必须每次新建timer都unref，计时器不阻止进程退出，对齐旧enroll行为
    this[idleTimer].unref();
  }

  addInHandler(handler) {
    this.inHandlers.push(handler);
  }

  addOutHandler(handler) {
    this.outHandlers.push(handler);
  }

  /**
   * 驱动入站责任链，循环执行所有inHandler
   * 单个handler异常将中断后续handler, handler返回 {break:true} 也会终止链路
   */
  async [runInChain](inHandlers, session, message, sessionHub) {
    let msg = message;
    for (const handler of inHandlers) {
      let res;
      try {
        res = await handler.handle(session, msg, sessionHub);
      } catch (err) {
        this.logger('error', 'dispatch', `runInChain:${handler.constructor.name}`, err);
        return;
      }
      // handler 返回了新的 message
      if (!res || typeof res !== 'object') continue;
      if (res.message !== undefined) msg = res.message;
      if (res.break === true) return;
    }
  }

  // 出站链路, 出错返回 undefined, 由 send 判断后不写 socket
  async [runOutChain](session, message, sessionHub) {
    let msg = message;
    for (const handler of this.outHandlers) {
      let res;
      try {
        res = await handler.handle(session, msg, sessionHub);
      } catch (err) {
        this.logger('error', 'send', `runOutChain:${handler.constructor.name}`, err);
        return undefined;
      }
      // handler 返回了新的 session / message，覆盖
      if (!res || typeof res !== 'object') continue;
      if (res.message !== undefined) msg = res.message;
      if (res.break === true) break;
    }
    return msg;
  }

  async dispatch(message) {
    const session = this.session;
    const sessionHub = this.sessionHub;
    const inHandlers = this.inHandlers;
    if (this.closed) {
      this.logger('warn', 'dispatch', 'channel closed');
      return;
    }

    this[resetIdleTimer](this.config.idleTimeout);

    const handle = async () => {
      // 一旦chain断开, 队列中的消息也会丢弃
      if (this.closed) return;
      this.logger('info', 'dispatch', Buffer.isBuffer(message) ? message.toString('hex').toUpperCase() : message);
      await this[runInChain](inHandlers, session, message, sessionHub);

      // 每条上行报文触发Redis续期
      if (!session || !sessionHub) return;
      try {
        await sessionHub.refreshSessionAlive(session);
      } catch (e) {
        this.logger('error', 'dispatch', 'refreshSessionAlive error', e);
      }
    }

    // 同一台设备接连上传 A B 两条数据，在 runInHandler 中两条数据都要 await 从 redis 中取数据出来处理完后再将数据存入 mysql
    // 不加串行队列是很有可能出现 先存了 B 再存 A 的情况
    // 串行队列会排队, 极端高频上报会内存堆积; 如果业务流量很大, 要慎重
    if (!this.config.enableDispatchQueue) {
      try {
        await handle(); 
      } catch (err) { 
        this.logger('error', 'dispatch', 'dispatch error', err); 
      }
      return;
    }

    this[dispatchQueue] = this[dispatchQueue].then(handle).catch(err=>{
      this.logger('error','dispatch', 'queue error', err);
    });
  }

  async send(message) {
    if (this.closed) {
      this.logger('warn', 'send', 'channel closed');
      return false;
    }

    this.logger('info', 'send', JSON.stringify(message));
    message = await this[runOutChain](this.session, message, this.sessionHub);
    if (message === undefined) return false;

    // await 期间可能已经断线
    if (this.closed || this.socket.destroyed) {
      this.logger('warn', 'send', 'channel closed before write');
      return false;
    }
    // socket.write 只接受 Buffer / string, 否则同步抛 ERR_INVALID_ARG_TYPE
    if (!Buffer.isBuffer(message) && typeof message !== 'string') {
      this.logger('error', 'send', 'outChain must return Buffer or string');
      return false;
    }
    this.logger('info', 'send', Buffer.isBuffer(message) ? message.toString('hex').toUpperCase() : message);
    // write在缓冲区满时返回false, 数据在排列(背压), 它不表示已经失败
    this.socket.write(message);
    return true;
  }

  close(auto, event = 'close', err) {
    this[resetIdleTimer](0);
    if (this.closed) return;
    this.closed = true;

    if (err instanceof Error) {
      this.logger('error', event, err);
    } else {
      this.logger('info', event);
    }

    this.socket.off('data', this._onData);
    this.socket.off('error', this._onError);
    this.socket.off('close', this._onClose);
    this.socket.off('end', this._onEnd);

    // 关键: 移除业务监听后, 必须保留一个空的 error 监听
    // 否则 destroy 之后残留的 write 回调、ECONNRESET 等 error 事件
    // 因为没有监听者, 会变成 uncaught exception 让进程崩溃
    this.socket.on('error', () => {});

    this.socket.destroy();
    this.emit('close', !!auto);

    // inHandlers/outHandlers 都只实例化了一次, 所有设备都是共用handler的
    // handler中处理逻辑都应该是无状态的, 如handle(session, msg, hub), 所以这里不能清除handler中的channel
    // for (const h of this.inHandlers) h.channel = null;
    // for (const h of this.outHandlers) h.channel = null;

    this.inHandlers = [];
    this.outHandlers = [];
    this.session = null;
    this.sessionHub = null;
    this.frameBuf = null;
    // 重置promise链引用, 帮助GC, 但中断不了已经pending的promise
    this[dispatchQueue] = Promise.resolve();
  }
}

module.exports = TcpChannel;
