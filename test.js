'use strict'

var test = require('tape').test
var persistence = require('./')
var Redis = require('ioredis')
var abs = require('aedes-persistence/abstract')
var db = new Redis()

db.on('connect', function() {
  db.connector.stream.unref()
})

abs({
  test: test,
  persistence: function () {
    db.flushall()
    return persistence()
  }
})

test('multiple persistences', function (t) {
  t.plan(5)

  var instance = persistence()
  var instance2 = persistence()
  var client = { id: 'abcde' }
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
    })
  })
})
