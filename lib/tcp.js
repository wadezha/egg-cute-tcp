'use strict';

const assert = require('assert');

const Utils = require('./utils');
const TcpServer = require('./server');
const loader = require('./loader');

module.exports = app => {
  app.addSingleton('tcp', createClient);
};

function createClient(config, app) {
  // port,idleTimeout,enableDispatchQueue,sessionKeyCacheKey,sessionIdCacheKey,parserPath,chainPath,inChain,outChain
  assert(config.port && Number.isInteger(config.idleTimeout) && config.idleTimeout > 0, `[egg-cute-tcp] 'port: ${config.port}', are required on config`);

  // 把全局 eventSend 拷贝一份给每个app实例
  config = Object.assign({}, config, { eventSend: app.config.tcp.eventSend, eventKickout: app.config.tcp.eventKickout, });

  const ctx = app.createAnonymousContext({ method: 'TCP', url: `/tcp`, });
  const server = new TcpServer(config, ctx);

  loader(config, app);

  const utils = new Utils(ctx, app);
  let chain = {};
  utils.flattenObj(app.tcpChain || {}, chain, null);
  chain = utils.recurseInstantiate(chain);
  server.parser = utils.recurseInstantiate(app.tcpParser || {});
  server.chain = utils.recurseInstantiate(app.tcpChain || {});

  for (const k of config.inChain) {
    assert(chain[k], `[egg-cute-tcp] inChain '${k}' not found`);
    server.useInHandler(chain[k]);
  }
  for (const k of config.outChain) {
    assert(chain[k], `[egg-cute-tcp] outChain '${k}' not found`);
    server.useOutHandler(chain[k]);
  }

  app.beforeStart(async () => {
    app.coreLogger.info(`[egg-cute-tcp] plugin loaded, port: ${config.port}`);
  });

  app.beforeClose(async () => {
    const closing = server.close().catch(() => {}); // 同步部分已停止接新连接
    await server.destroy();                         // 踢掉现有会话
    await closing;
  });

  return server;
}
