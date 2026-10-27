
const events = require('events');

class Session extends events.EventEmitter {
  constructor(channel) {
    super();
    this.destroyed = false;
    this.sessionId = 0;
    this.sessionKey = ''; // imei
    this.connectTime = null;
    this.registerTime = null;
    this.clientIp = '';
    this.clientPort = null;

    // 监听channel关闭事件
    // 1. TCP 断开 → channel.close () → channel.emit ('close')
    // 2. session 收到 channel 的 close 事件 → 清掉绑定的channel → session.emit ('close')
    // 3. sessionManager监听了 session 的close事件, 收到后把 session 从内存和 redis 清理掉
    this.channel = channel;
    this._onEnd = this.end.bind(this);
    this.channel.on('close', this._onEnd);

    this.channel.session = this;
  }

  async send() {
    if (this.destroyed || !this.channel) return false;
    return await this.channel.send.apply(this.channel, arguments);
  }

  // 业务踢下线：调用 session.close, 底层 TCP 会断开
  // session.close() → socketChannel.close() → socket.destroy()
  close(auto) {
    if (this.destroyed || !this.channel) return false;
    this.channel.close(!!auto);
    return true;
  }

  destroy() {
    if (this.destroyed || !this.channel) return;
    this.channel.close(false);
  }

  end(auto) {
    if (this.destroyed) return;
    this.destroyed = true;
    if(this.channel) {
      // 构造函数中监听了channel的close, 所以此处要移除监听
      this.channel.off('close', this._onEnd);
      this.channel.session = null;
    }
    this.channel = null;
    this.emit('close', auto);
  }

  toString() {
    const sessionJson = {
      sessionId: this.sessionId, 
      sessionKey: this.sessionKey, 
      connectTime: this.connectTime, 
      registerTime: this.registerTime, 
      clientIp: this.clientIp, 
      clientPort: this.clientPort,
    }
    return JSON.stringify(sessionJson);
  }
}

module.exports = Session;
