'use strict';

class ConsumeStats {
  /**
   * @param {Map} offsetTable - offset table
   * @param {Number} consumeTps - consume tps
   */
  constructor(offsetTable, consumeTps) {
    // 队列 offset 信息, Map<MessageQueue, OffsetWrapper>
    this.offsetTable = offsetTable || new Map();
    // 消费速度
    this.consumeTps = consumeTps || 0;
  }

  /**
   * 计算总堆积数
   * @return {Number} - MessageQueue 总堆积数
   */
  computeTotalDiff() {
    let diffTotal = 0;

    for (const offset of this.offsetTable.values()) {
      const diff = offset.brokerOffset - offset.consumerOffset;
      diffTotal += diff;
    }

    return diffTotal;
  }

  /**
   * 返回每个 queue 的堆积数
   * @return {Array<Object>} - [{ queue: String, diff: Number }] 每个队列的堆积数
   */
  getQueuesDiff() {
    const queuesDiff = [];

    for (const mqStr of this.offsetTable.keys()) {
      const offset = this.offsetTable.get(mqStr);
      const diff = offset.brokerOffset - offset.consumerOffset;
      queuesDiff.push({ queue: mqStr, diff });
    }

    return queuesDiff;
  }
}

module.exports = ConsumeStats;
