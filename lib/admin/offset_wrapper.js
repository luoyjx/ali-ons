'use strict';

class OffsetWrapper {
  constructor(options) {
    const defaults = {
      brokerOffset: null,
      consumerOffset: null,
      lastTimestamp: null,
    };

    Object.assign(this, defaults, options);
  }
}

module.exports = OffsetWrapper;
