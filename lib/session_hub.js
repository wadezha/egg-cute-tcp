
const events = require('events');
const clientConnected = Symbol('cuteTcp#clientConnected');
const onClientConnected = Symbol('cuteTcp#onClientConnected');
const clientSend = Symbol('cuteTcp#clientSend');
const onClientSend = Symbol('cuteTcp#onClientSend');
const clientKickout = Symbol('cuteTcp#clientKickout');
const onClientKickout = Symbol('cuteTcp#onClientKickout');

class SessionHub extends events.EventEmitter {
  constructor(config, ctx) {
    super();
    // sessionKeyCacheKey: tcp:swd:session:key, sessionIdCacheKey: tcp:swd:session:id
    this.config = config;
    this.ctx = ctx;
    this.redis = config.sessionRedisClient ? ctx.app.redis.get(config.sessionRedisClient) : ctx.app.redis;

    this.sessions = new Map();
    this.keyToIds = new Map();

    this[onClientConnected] = this[clientConnected].bind(this);
    this.ctx.app.on('cute-tcp:client-connected', this[onClientConnected]);
    this[onClientSend] = this[clientSend].bind(this);
    this.ctx.app.on('cute-tcp:client-send', this[onClientSend]);
    this[onClientKickout] = this[clientKickout].bind(this);
    this.ctx.app.on('cute-tcp:client-kickout', this[onClientKickout]);

    // 记录每个session上一次redis续期时间戳 ms
    this.lastRenew = new Map();
    // 节流间隔: idleTimeout的一半，例如10min超时，则5min才允许续一次redis
    this.renewInterval = Math.floor(this.config.idleTimeout * 0.5);
  }

  redisTtl() {
    return Math.ceil((this.config.idleTimeout + this.renewInterval) / 1000);
  }

  /**
   * 收到设备数据/心跳，触发续期（带节流，不会狂打redis）
   */
  async refreshSessionAlive(session) {
    const { sessionId, sessionKey } = session;
    if (!this.sessions.has(sessionId)) return;

    const now = Date.now();
    const last = this.lastRenew.get(sessionId) || 0;
    // 时间没到，直接跳过redis调用
    if (now - last < this.renewInterval) return;

    this.lastRenew.set(sessionId, now);

    try {
      const luaScript = `for _,hk in ipairs(KEYS) do redis.call('EXPIRE',hk,tonumber(ARGV[1])) end return 1`;
      const vals = sessionKey ? [`${this.config.sessionKeyCacheKey}:${sessionKey}`, `${this.config.sessionIdCacheKey}:${sessionId}`] : [`${this.config.sessionIdCacheKey}:${sessionId}`];
      await this.redis.eval(luaScript, vals.length, ...vals, this.redisTtl());
    } catch (err) { 
      // 续期失败: 回滚, 下一条上行报文可以马上重试
      // 先判断还是自己写的值: 期间 session 可能已经关闭, closeSession 删掉了这条记录, 不能再写回去造成残留
      if (this.lastRenew.get(sessionId) === now) this.lastRenew.set(sessionId, last);
      this.logger('error', 'refreshSessionAlive', `sessionId: ${sessionId} ${sessionKey}, expire error`, err);
    }
  }

  logger(level, event, ...data) {
    this.ctx.url = `/tcp/${event}`;
    this.ctx.logger[level](`[egg-cute-tcp] ${event} `, ...data);
  }

  // 集群模式下, 设备断开重连可能从集群A节点分配到B节点中, 旧 session 残留在A节点 内存, 成为僵尸session (所有语言都有这个问题)
  // 方案: 1. 空闲超时触发清除 2. TCP Keepalive 内核探测触发清除 
  // 3. 设备上线通过redis pub向 其他worker 发布`cute-tcp:pub_sub`事件, worker收到事件踢掉该client在worker的旧会话
  async addSession(session) {
    const sessionId = session.sessionId;
    if (this.sessions.has(sessionId)) return;

    session.once('close', async () => { await this.closeSession(sessionId); });
    this.sessions.set(sessionId, session);
    this.lastRenew.set(sessionId, Date.now());
    try {
      await this.redis.set(`${this.config.sessionIdCacheKey}:${sessionId}`, session.toString(), 'EX', this.redisTtl());
    } catch(err) {
      this.logger('error', 'addSession', `sessionId: ${sessionId}, redis error`, err);
    }
  }

  async closeSession(sessionId) {
    const session = this.sessions.get(sessionId);
    const sessionKey = session && session.sessionKey ? session.sessionKey : '';
    if (sessionKey && this.keyToIds.get(sessionKey) === sessionId) this.keyToIds.delete(sessionKey);
    if (session) this.sessions.delete(sessionId);
    this.lastRenew.delete(sessionId);

    try {
      if (sessionKey) {
        const luaScript = `if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[1]) end redis.call('DEL', KEYS[2]) return 1`;
        await this.redis.eval(luaScript, 2, `${this.config.sessionKeyCacheKey}:${sessionKey}`, `${this.config.sessionIdCacheKey}:${sessionId}`, String(sessionId));
      } else {
        await this.redis.del(`${this.config.sessionIdCacheKey}:${sessionId}`);
      }
    } catch(err) {
      this.logger('error', 'closeSession', `sessionId: ${sessionId} ${sessionKey}, redis error`, err);
    }
    this.logger('info', 'closeSession', `clear session, sessionId: ${sessionId} ${sessionKey}`);
  }

  // public
  async register(session) {
    if (session.destroyed) return;

    const { sessionId, sessionKey } = session;
    if (!sessionKey) return;

    const existSessionId = this.keyToIds.get(sessionKey);
    if (existSessionId && existSessionId === sessionId) {
      this.logger('warn', 'register', `session registered, sessionId: ${sessionId} ${sessionKey}`);
      return;
    }

    session.registerTime = new Date().toISOString().replace('T', ' ').slice(0, 19);
    this.keyToIds.set(sessionKey, sessionId);
    this.sessions.set(sessionId, session);

    try {
      const luaScript = `for i,hk in ipairs(KEYS) do local val=ARGV[i];local ttl=tonumber(ARGV[#ARGV]);redis.call('SET',hk,val,'EX',ttl) end return 1`;
      await this.redis.eval(luaScript, 2, `${this.config.sessionKeyCacheKey}:${sessionKey}`, `${this.config.sessionIdCacheKey}:${sessionId}`, `${sessionId}`, session.toString(), this.redisTtl());
      // 发布上线广播
      await this.ctx.app.tcpPubSub.publish({ type: 'client.connected', protocol: this.config.protocol, sessionKey, newSessionId: sessionId });
    } catch(err) {
      this.logger('error', 'register', `sessionId: ${sessionId} ${sessionKey}, error`, err);
    }

    if (!existSessionId) return;
    const existSession = this.sessions.get(existSessionId);
    if (!existSession) return;
    existSession.close(false);
    this.logger('warn', 'register', `exist session kickout, sessionId: ${existSessionId} ${existSession.sessionKey}`);
  }

  // 处理app派发的集群上线事件
  [clientConnected](message) {
    const { protocol, sessionKey, newSessionId } = message;
    // 不是本端口的消息直接丢弃，不同端口协议设备互不干扰
    if (`${protocol}` !== `${this.config.protocol}`) return;

    const localSessionId = this.keyToIds.get(sessionKey);
    if(!localSessionId) return;
    // 若只判断localSessionId与newSessionId是否相等为有以下场景导致设备直接下线
    // 设备连B节点 注册并广播 <key, sessionId2>
    // 广播未传到A节点时, 设备断了, 重连到了A节点 注册并广播 <key, sessionId3>
    // A节点收到B节点广播 <key, sessionId2>, A节点判断 sessionId3 != sessionId2, 设备下线
    // B节点收到A节点广播 <key, sessionId3>, B节点判断 sessionId2 != sessionId3, 设备也下线
    // 所以要判断, 只踢比新上线的更旧的; 相等(自己发的)或更新的都不动
    if (localSessionId >= newSessionId) return;

    const existSession = this.getSession(sessionKey);
    if(!existSession) return;
    existSession.close(false);
    this.logger('warn', 'clientConnected', `kickout local sessionId: ${localSessionId} ${sessionKey}`);
  }

  // public
  getSession(sessionKey) {
    const sessionId = this.keyToIds.get(sessionKey);
    if (!sessionId) return null;
    return this.sessions.get(sessionId);
  }

  // public
  // 双向 TLS 时读取设备证书; 连接已断开或不是 TLS 连接返回 null; 设备没有出示证书时 Node 返回 {}
  getPeerCertificate(sessionId) {
    const session = this.sessions.get(sessionId);
    const socket = session && session.channel && session.channel.socket;
    if (!socket || typeof socket.getPeerCertificate !== 'function') return null;
    return socket.getPeerCertificate();
  }

  // public
  getKeys() {
    return Array.from(this.keyToIds.keys());
  }

  // 踢掉设备: 设备在本进程直接断开; 在其他 worker / 机器上则发布踢人命令
  // 返回值: 本进程时是 close 的真实结果; 经 pubsub 转发时 true 只表示命令已发布
  // 非本库的程序只能发布 pubsub 消息(JSON 文本): { type: <config.eventKickout>, protocol, sessionKey }
  async kickout(sessionKey) {
    try {
      // 1. 设备在本进程, 直接发, 返回真实的写入结果
      const session = this.getSession(sessionKey);
      if (session) return session.close(false);

      // 2. 其他 worker / 机器上有没有: 看 Redis 里的在线记录
      const isOnline = await this.redis.exists(`${this.config.sessionKeyCacheKey}:${sessionKey}`);
      if (!isOnline) return false;

      // 3. 发布命令, 持有这个 session 的 worker 去发送
      await this.ctx.app.tcpPubSub.publish({ type: this.config.eventKickout, protocol: this.config.protocol, sessionKey });
      return true;
    } catch (err) {
      this.logger('error', 'kickout', `sessionKey: ${sessionKey}`, err);
      return false;
    }
  }

  // public
  // 只可在 handler 或使用本库的程序中使用; 非本库的程序需向设备发消息, 只能发布 pubsub 消息(JSON 文本):
  //   { type: <config.eventSend>, protocol, sessionKey, message }
  //   踢人: { type: <config.eventKickout>, protocol, sessionKey }
  //   redis: 发布到 pubSubChannel 频道; rabbit: 发布到名为 pubSubChannel 的 fanout exchange
  // 返回值: 设备在本进程时是真实的写入结果; 经 pubsub 转发时 true 只表示命令已发布, 不代表设备收到
  async send(sessionKey, message) {
    try {
      // 1. 设备在本进程, 直接发, 返回真实的写入结果
      const session = this.getSession(sessionKey);
      if (session) return await session.send(message);

      // 2. 其他 worker / 机器上有没有: 看 Redis 里的在线记录
      const isOnline = await this.redis.exists(`${this.config.sessionKeyCacheKey}:${sessionKey}`);
      if (!isOnline) return false;

      // 3. 发布命令, 持有这个 session 的 worker 去发送
      await this.ctx.app.tcpPubSub.publish({ type: this.config.eventSend, protocol: this.config.protocol, sessionKey, message });
      return true;
    } catch (err) {
      this.logger('error', 'send', `sessionKey: ${sessionKey}`, err);
      return false;
    }
  }

  async [clientSend]({ protocol, sessionKey, message }) {
    if (`${protocol}` !== `${this.config.protocol}`) return;
    const session = this.getSession(sessionKey);
    if (!session) return; // 不在本进程, 忽略

    try {
      await session.send(message);
    } catch (err) {
      this.logger('error', 'clientSend', `sessionKey: ${sessionKey}`, err);
    }
  }

  [clientKickout]({ protocol, sessionKey }) {
    if (`${protocol}` !== `${this.config.protocol}`) return;
    const session = this.getSession(sessionKey);
    if (!session) return; // 不在本进程, 忽略
    session.close(false);
    this.logger('warn', 'clientKickout', `kick by api, sessionKey: ${sessionKey}`);
  }

  close() {
    for(const session of this.sessions.values()) {
      session.close();
    }
  }

  destroy() {
    this.ctx.app.off('cute-tcp:client-connected', this[onClientConnected]);
    this.ctx.app.off('cute-tcp:client-send', this[onClientSend]);
    this.ctx.app.off('cute-tcp:client-kickout', this[onClientKickout]);
    this.close();
    this.lastRenew.clear();
  }
}

module.exports = SessionHub;
