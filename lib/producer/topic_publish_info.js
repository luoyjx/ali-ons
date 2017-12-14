'use strict';

class TopicPublishInfo {
  constructor() {
    this.orderTopic = false;
    this.haveTopicRouterInfo = false;
    this.messageQueueList = [];
    this.sendWhichQueue = 0;
  }

  /**
   * 是否可用
   */
  get ok() {
    return this.messageQueueList && this.messageQueueList.length;
  }

  /**
   * 如果lastBrokerName不为null，则寻找与其不同的MessageQueue
   * @param {String} lastBrokerName - last broker name
   * @return {Object} queue
   */
  selectOneMessageQueue(lastBrokerName) {
    let index,
      pos,
      mq;
    if (lastBrokerName) {
      index = this.sendWhichQueue++;
      for (let i = 0, len = this.messageQueueList.length; i < len; i++) {
        pos = Math.abs(index++) % len;
        mq = this.messageQueueList[pos];
        if (mq.brokerName !== lastBrokerName) {
          return mq;
        }
      }
      return null;
    }
    index = this.sendWhichQueue++;
    pos = Math.abs(index) % this.messageQueueList.length;
    return this.messageQueueList[pos];
  }

  /**
   * 根据 ShardingKey 选择发送
   * @param {String} shardingKey - sharding key
   * @return {Object} queue
   */
  selectOneMessageQueueByShardingKey (shardingKey) {
    if (!shardingKey) {
      return null;
    }

    let hash = 0;

    for (let i = 0, len = shardingKey.length; i < len; i++) {
      hash = ((hash << 5) - hash) + shardingKey.charCodeAt(i);
      hash = hash & hash;
    }

    let select = Math.abs(hash);

    if (select < 0) {
      select = 0;
    }

    return this.messageQueueList[select % this.messageQueueList.length];
  }
}

module.exports = TopicPublishInfo;
