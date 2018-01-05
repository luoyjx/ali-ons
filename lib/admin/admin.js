'use strict';

const debug = require('debug')('mq:admin');
const co = require('co');
const MixAll = require('../mix_all');
const MQClient = require('../mq_client');
const ClientConfig = require('../client_config');

const defaultOptions = {
  producerGroup: MixAll.DEFAULT_PRODUCER_GROUP,
  createTopicKey: MixAll.DEFAULT_TOPIC,
  pullConsumeStatsInterval: 5000, // 拉取消费状态间隔
};

class Admin extends ClientConfig {
  /**
   * rocketmq message admin
   * @param {Object} options -
   * @constructor
   */
  constructor(options) {
    super(Object.assign({ initMethod: 'init' }, defaultOptions, options));

    this._inited = false;
    this._mqClient = MQClient.getAndCreateMQClient(this);
    this._mqClient.on('error', err => this._error(err));
  }

  /**
   * start the admin
   * @return {void}
   */
  * init() {
    yield this._mqClient.ready();
    debug('[mq:admin] admin started');
    this._inited = true;
  }

  /**
   * close the admin
   * @return {Promise} promise
   */
  close() {
    return co(function* () {
      yield this._mqClient.close();
      this._inited = false;
      this.logger.info('[mq:admin] admin close');
    }.bind(this));
  }

  getConsumeStats(topic, consumeGroup) {
    return co(this._mqClient.getConsumeStatsTask(topic, consumeGroup));
  }

  /**
   * default error handler
   * @param {Error} err - error
   * @return {void}
   */
  _error(err) {
    setImmediate(() => {
      err.message = 'MQAdmin occurred an error' + err.message;
      this.emit('error', err);
    });
  }
}

module.exports = Admin;
