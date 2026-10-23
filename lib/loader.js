'use strict';

const assert = require('assert');
const path = require('path');

module.exports = (config, app) => {

  assert(config, '[egg-cute-tcp] config invalid');

  // 每个 client 都从空开始, 没配路径时不会拿到上一个 client 的结果
  app.tcpParser = {};
  app.tcpChain = {};

  if (config.parserPath) {
    const directory = path.join(app.config.baseDir, config.parserPath);
    app.loader.loadToApp(directory, 'tcpParser');
  }

  if (config.chainPath) {
    const directory = path.join(app.config.baseDir, config.chainPath);
    app.loader.loadToApp(directory, 'tcpChain');
  }

  app.cuteTcp = {};
  Object.defineProperty(app.cuteTcp, 'middleware', {
    configurable: true,
    enumerable: true,
    get() {
      app.deprecate('app.cuteTcp.middleware has been deprecated, please use app.tcpChain instead!');
      return app.tcpChain;
    },
  });
};
