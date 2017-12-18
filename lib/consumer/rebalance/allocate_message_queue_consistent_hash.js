'use strict';

const debug = require('debug')('mq:allocate');
const HashRing = require('hashring');
const AllocateMessageQueueStrategy = require('./allocate_message_queue_strategy');

class AllocateMessageQueueConsistentHash extends AllocateMessageQueueStrategy {
  get name() {
    return 'CONSISTENT_HASH';
  }

  allocate(consumerGroup, currentCID, mqAll, cidAll) {
    if (!currentCID || currentCID.length < 1) {
      throw new Error('currentCID is empty');
    }
    if (!mqAll || !mqAll.length) {
      throw new Error('mqAll is null or mqAll empty');
    }
    if (!cidAll || !cidAll.length) {
      throw new Error('cidAll is null or cidAll empty');
    }

    const result = [];
    const index = cidAll.indexOf(currentCID);
    if (index === -1) { // 不存在此ConsumerId ,直接返回
      debug('[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %j', //
        consumerGroup, //
        currentCID, //
        cidAll);
      return result;
    }

    const ring = new HashRing(cidAll);

    for (const mq of mqAll) {
      const clientId = ring.get(mq.key);
      if (clientId && currentCID === clientId) {
        result.push(mq);
      }
    }

    return result;
  }
}

module.exports = AllocateMessageQueueConsistentHash;
