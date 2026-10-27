'use strict';

exports.tcp = {
  app: true,
  pubSubType: 'redis',
  pubSubClient: '',
  pubSubChannel: 'cute-tcp:pub_sub',
  eventSend: 'client.send',
  eventKickout: 'client.kickout',
  client: {
    port: 1883,
    protocol: 'ect',
    enableDispatchQueue: false,
    idleTimeout: 300,
    keepAlive: 60 * 1000,
    sessionRedisClient: '',
    sessionKeyCacheKey: 'tcp:{ect}:session:key',
    sessionIdCacheKey: 'tcp:{ect}:session:id',
    parserPath: '',
    chainPath: '',
    inChain: [],
    outChain: [],
    tls: {
      enable: false,
    },
  },
  // clients: {
  //   bsj: {
  //     port: 1882,
  //     enableDispatchQueue: false,
  //     idleTimeout: 300,
  //     keepAlive: 60 * 1000,
  //     sessionRedisClient: '',
  //     sessionKeyCacheKey: 'tcp:{bsj}:session:key',
  //     sessionIdCacheKey: 'tcp:{bsj}:session:id',
  //     parserPath: '',
  //     chainPath: '',
  //     inChain: [],
  //     outChain: [],
  //     tls: {
  //      enable: false,
  //     },
  //   },
  //   swd: {
  //     port: 1881,
  //     enableDispatchQueue: false,
  //     idleTimeout: 300,
  //     keepAlive: 60 * 1000,
  //     sessionRedisClient: '',
  //     sessionKeyCacheKey: 'tcp:{swd}:session:key',
  //     sessionIdCacheKey: 'tcp:{swd}:session:id',
  //     parserPath: '',
  //     chainPath: '',
  //     inChain: [],
  //     outChain: [],
  //     tls: {
  //      enable: false,
  //     },
  //   },
  // }
};
