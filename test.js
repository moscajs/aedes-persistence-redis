'use strict'

var test = require('tape').test
var persistence = require('./')
var Redis = require('ioredis')
var mqemitterRedis = require('mqemitter-redis')
var abs = require('aedes-cached-persistence/abstract')
var db = new Redis()

db.on('error', function (e) {
  console.trace(e)
})

db.on('connect', unref)

function unref () {
  this.connector.stream.unref()
}

test('external Redis conn', function (t) {
  t.plan(2)

  var externalRedis = new Redis()
  var emitter = mqemitterRedis()

  db.on('error', function (e) {
    t.notOk(e)
  })

  db.on('connect', function () {
    t.pass('redis connected')
  })
  var instance = persistence({
    conn: externalRedis
  })

  instance.broker = toBroker('1', emitter)

  instance.on('ready', function () {
    t.pass('instance ready')
    externalRedis.disconnect()
    instance.destroy()
    emitter.close()
  })
})

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
  },
  waitForReady: true
})

function toBroker (id, emitter) {
  return {
    id: id,
    publish: emitter.emit.bind(emitter),
    subscribe: emitter.on.bind(emitter),
    unsubscribe: emitter.removeListener.bind(emitter)
  }
}

test('packet ttl', function (t) {
  t.plan(4)
  db.flushall()
  var emitter = mqemitterRedis()
  var instance = persistence({
    packetTTL: function () {
      return 1
    }
  })
  instance.broker = toBroker('1', emitter)

  var subs = [{
    clientId: 'ttlTest',
    topic: 'hello',
    qos: 1
  }]
  var packet = {
    cmd: 'publish',
    topic: 'hello',
    payload: 'ttl test',
    qos: 1,
    retain: false,
    brokerId: instance.broker.id,
    brokerCounter: 42
  }
  instance.outgoingEnqueueCombi(subs, packet, function enqueued (err, saved) {
    t.notOk(err)
    t.deepEqual(saved, packet)
    setTimeout(function () {
      var offlineStream = instance.outgoingStream({ id: 'ttlTest' })
      offlineStream.on('data', function (offlinePacket) {
        t.notOk(offlinePacket)
      })
      offlineStream.on('end', function () {
        instance.destroy(t.pass.bind(t, 'stop instance'))
        emitter.close(t.pass.bind(t, 'stop emitter'))
      })
    }, 1100)
  })
})

test('multiple persistences', function (t) {
  t.plan(7)
  db.flushall()
  var emitter = mqemitterRedis()
  var emitter2 = mqemitterRedis()
  var instance = persistence()
  var instance2 = persistence()
  instance.broker = toBroker('1', emitter)
  instance2.broker = toBroker('2', emitter2)

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

  var gotSubs = false
  var addedSubs = false

  function close () {
    if (gotSubs && addedSubs) {
      instance.destroy(t.pass.bind(t, 'first dies'))
      instance2.destroy(t.pass.bind(t, 'second dies'))
      emitter.close(t.pass.bind(t, 'first emitter dies'))
      emitter2.close(t.pass.bind(t, 'second emitter dies'))
    }
  }

  instance2._waitFor(client, 'sub_' + 'hello', function () {
    instance2.subscriptionsByTopic('hello', function (err, resubs) {
      t.notOk(err, 'subs by topic no error')
      t.deepEqual(resubs, [{
        clientId: client.id,
        topic: 'hello/#',
        qos: 1
      }, {
        clientId: client.id,
        topic: 'hello',
        qos: 1
      }])
      gotSubs = true
      close()
    })
  })

  var ready = false
  var ready2 = false

  function addSubs () {
    if (ready && ready2) {
      instance.addSubscriptions(client, subs, function (err) {
        t.notOk(err, 'add subs no error')
        addedSubs = true
        close()
      })
    }
  }

  instance.on('ready', function () {
    ready = true
    addSubs()
  })

  instance2.on('ready', function () {
    ready2 = true
    addSubs()
  })
})

test('unknown cache key', function (t) {
  t.plan(3)
  db.flushall()
  var emitter = mqemitterRedis()
  var instance = persistence()
  var client = { id: 'unknown_pubrec' }

  instance.broker = toBroker('1', emitter)

  // packet with no brokerId
  var packet = {
    cmd: 'pubrec',
    topic: 'hello',
    qos: 2,
    retain: false
  }

  function close () {
    instance.destroy(t.pass.bind(t, 'instance dies'))
    emitter.close(t.pass.bind(t, 'emitter dies'))
  }

  instance.outgoingUpdate(client, packet, function (err, client, packet) {
    t.equal(err.message, 'unknown key', 'Received unknown PUBREC')
    close()
  })
})
