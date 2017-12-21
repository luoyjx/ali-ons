ali-ons
=======

[![NPM version][npm-image]][npm-url]
[![build status][travis-image]][travis-url]
[![David deps][david-image]][david-url]
[![node version][node-image]][node-url]

[npm-image]: https://img.shields.io/npm/v/ali-ons.svg?style=flat-square
[npm-url]: https://npmjs.org/package/ali-ons
[travis-image]: https://img.shields.io/travis/ali-sdk/ali-ons.svg?style=flat-square
[travis-url]: https://travis-ci.org/ali-sdk/ali-ons
[david-image]: https://img.shields.io/david/ali-sdk/ali-ons.svg?style=flat-square
[david-url]: https://david-dm.org/ali-sdk/ali-ons
[node-image]: https://img.shields.io/badge/node.js-%3E=_4.2.3-green.svg?style=flat-square
[node-url]: http://nodejs.org/download/

Aliyun Open Notification Service Client (base on opensource project [RocketMQ](https://github.com/alibaba/RocketMQ/tree/master/rocketmq-client))

Sub module of [ali-sdk](https://github.com/ali-sdk/ali-sdk).

## Install

```bash
npm install ali-ons --save
```

## Usage

consumer

```js
'use strict';

const httpclient = require('urllib');
const Consumer = require('ali-ons').Consumer;
const consumer = new Consumer({
  httpclient,
  accessKey: 'your-accesskey',
  secretKey: 'your-secretkey',
  consumerGroup: 'your-consumer-group',
  // isBroadcast: true,
});

consumer.subscribe(config.topic, '*', function*(msg) {
  console.log(`receive message, msgId: ${msg.msgId}, body: ${msg.body.toString()}`)
});

consumer.on('error', err => console.log(err));
```

producer

```js
'use strict';

const co = require('co');
const httpclient = require('urllib');
const Producer = require('ali-ons').Producer;
const Message = require('ali-ons').Message;

const producer = new Producer({
  httpclient,
  accessKey: 'your-accesskey',
  secretKey: 'your-secretkey',
  producerGroup: 'your-producer-group',
});

producer.ready(() => {
  console.log('producer ready');
  const msg = new Message('your-topic', // topic
    'TagA', // tag
    'Hello ONS !!! ' // body
  );

  // Might be an order id or other key you want
  const shardingKey = 'your sharding key goes here'

  co(function *() {
    // Send normal message, message will delivery averagely into topic queues
    let sendResult = yield * producer.send(msg);

    // Send sharding message
    // Messages will delivery by sharding key, that means the messages with
    // same sharding key will always be deliveried into same queue, but you
    // should handle the situation when consumers scaling yourself.  
    sendResult = yield * producer.send(msg, shardingKey);
  });
});
```

Rocketmq

implemention from [yeliex/rocketmq](https://github.com/yeliex/rocketmq)

consumer 

```js
const consumer = new Consumer({
  namesrvAddr: 'localhost:9876', // for rocket mq
  accessKey: 'your-accesskey',  // for aliyun-ons
  secretKey: 'your-secretkey',  // for aliyun-ons
  onsAddr: '',                   // for aliyun-ons
  consumerGroup: 'your-consumer-group',  // for aliyun-ons
  
  isBroadcast: false, // default is false, that mean messages will be pushed to consumer cluster only once.
});
```

producer

```js
const producer = new Producer({
  namesrvAddr: 'localhost:9876', // for rocket mq
  accessKey: 'your-accesskey',   // for aliyun-ons
  secretKey: 'your-secretkey',    // for aliyun-ons
  producerGroup: 'your-producer-group',  // for aliyun-ons
});
```

## License

[MIT](LICENSE)
