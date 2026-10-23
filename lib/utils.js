'use strict';

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
}
module.exports = Utils;
