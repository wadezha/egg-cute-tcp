"use strict";

class Chain {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async handle(session, message, sessionHub) {
    throw new Error('SubClass must impl this');
  }
}

module.exports = Chain;
