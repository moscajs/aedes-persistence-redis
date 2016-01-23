'use strict'

var test = require('tape').test
var persistence = require('./')
var Redis = require('ioredis')
var mqemitterRedis = require('mqemitter-redis')
var abs = require('aedes-persistence/abstract')
var db = new Redis()

db.on('connect', unref)

function unref () {
  this.connector.stream.unref()
}

abs({
  test: test,
  buildEmitter: function () {
    var emitter = mqemitterRedis()

    emitter.subConn.on('connect', unref)
    emitter.pubConn.on('connect', unref)

    return emitter
  },
  persistence: function () {
    db.flushall()
    return persistence()
  }
})

test('multiple persistences', function (t) {
  t.plan(7)
  db.flushall()
  var emitter = mqemitterRedis()
  var emitter2 = mqemitterRedis()
  var instance = persistence()
  var instance2 = persistence()
  instance.broker = {
    id: '1',
    publish: emitter.emit.bind(emitter),
    subscribe: emitter.on.bind(emitter)
  }
  instance2.broker = {
    id: '2',
    publish: emitter2.emit.bind(emitter2),
    subscribe: emitter2.on.bind(emitter2)
  }
  var client = { id: 'multipleTest' }
  var subs = [{
    topic: 'hello',
    qos: 1
  }, {
    topic: 'hello/#',
    qos: 1
  }, {
    topic: 'matteo',
    qos: 1
  }]

  instance.addSubscriptions(client, subs, function (err) {
    t.notOk(err, 'no error')
    instance2.subscriptionsByTopic('hello', function (err, resubs) {
      t.notOk(err, 'no error')
      t.deepEqual(resubs, [{
        clientId: client.id,
        topic: 'hello/#',
        qos: 1
      }, {
        clientId: client.id,
        topic: 'hello',
        qos: 1
      }])
      instance.destroy(t.pass.bind(t, 'first dies'))
      instance2.destroy(t.pass.bind(t, 'second dies'))
      emitter.close(t.pass.bind(t, 'first emitter dies'))
      emitter2.close(t.pass.bind(t, 'second emitter dies'))
    })
  })
})
