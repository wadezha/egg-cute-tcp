'use strict';
const fs = require('fs');
const path = require('path');

class Utils {
  constructor(ctx, app) {
    this.ctx = ctx;
    this.app = app;
  }

  recurseInstantiate(val) {
    if (typeof val === 'function') {
      return new val(this.ctx, this.app);
    }
    if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
      const res = {};
      for (const k of Object.keys(val)) res[k] = this.recurseInstantiate(val[k]);
      return res;
    }
    return val;
  }

  flattenObj(source, to, newKey) {
    for (const key of Object.keys(source)) {
      const combineKey = newKey ? `${newKey}.${key}` : key;
      if (source[key] !== null && typeof source[key] === "object" && !Array.isArray(source[key])) {
        this.flattenObj(source[key], to, combineKey);
        continue;
      }
      to[combineKey] = source[key];
    }
  }

  // key / cert / ca / pfx can be a file path (absolute or relative to the app base dir), a PEM string, a Buffer,
  // or Node's object form { pem | buf, passphrase } (pem / buf follow the same rules)
  readPem(value, baseDir) {
    if (Array.isArray(value)) return value.map(v => this.readPem(v, baseDir));
    // { pem, passphrase } / { buf, passphrase }: 保留 passphrase 等其他字段, 只解析 pem / buf
    if (value !== null && typeof value === 'object' && !Buffer.isBuffer(value)) {
      const out = { ...value };
      for (const k of ['pem', 'buf']) {
        if (out[k] !== undefined) out[k] = this.readPem(out[k], baseDir);
      }
      return out;
    }
    if (Buffer.isBuffer(value) || /-----BEGIN /.test(value)) return value;
    return fs.readFileSync(path.resolve(baseDir, value));
  }
}
module.exports = Utils;
