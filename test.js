const test = require('node:test')
const persistence = require('./')
const Redis = require('ioredis')
const mqemitterRedis = require('mqemitter-redis')
const abs = require('aedes-cached-persistence/abstract')
const { QlobberTrue } = require('qlobber')
function sleep (sec) {
  return new Promise(resolve => setTimeout(resolve, sec * 1000))
}

function waitForEvent (obj, resolveEvt) {
  return new Promise((resolve, reject) => {
    obj.once(resolveEvt, () => {
      resolve()
    })
    obj.once('error', reject)
  })
}

function setUpPersistence (t, id, persistenceOpts) {
  const emitter = mqemitterRedis()
  const instance = persistence(persistenceOpts)
  instance.broker = toBroker(id, emitter)
  t.diagnostic(`instance ${id} created`)
  return { instance, emitter, id }
}

function cleanUpPersistence (t, { instance, emitter, id }) {
  instance.destroy()
  emitter.close()
  t.diagnostic(`instance ${id} destroyed`)
}

function toBroker (id, emitter) {
  return {
    id,
    publish: emitter.emit.bind(emitter),
    subscribe: emitter.on.bind(emitter),
    unsubscribe: emitter.removeListener.bind(emitter)
  }
}
function unref () {
  this.connector.stream.unref()
}

// testing starts here
const db = new Redis()
db.on('error', e => {
  console.trace(e)
})
db.on('connect', unref)

test('external Redis conn', async t => {
  t.plan(2)
  const externalRedis = new Redis()
  await waitForEvent(externalRedis, 'connect')
  t.diagnostic('redis connected')
  t.assert.ok(true, 'redis connected')
  const p = setUpPersistence(t, '1', {
    conn: externalRedis
  })
  await waitForEvent(p.instance, 'ready')
  t.assert.ok(true, 'instance ready')
  t.diagnostic('instance ready')
  externalRedis.disconnect()
  cleanUpPersistence(t, p)
})

abs({
  test,
  buildEmitter () {
    const emitter = mqemitterRedis()
    emitter.subConn.on('connect', unref)
    emitter.pubConn.on('connect', unref)

    return emitter
  },
  persistence: () => {
    db.flushall()
    return persistence()
  },
  waitForReady: true
})

test('packet ttl', async t => {
  t.plan(3)
  // the promise is required for the test to wait for the end event
  const executeTest = new Promise((resolve, reject) => {
    db.flushall()

    const p = setUpPersistence(t, '1', {
      packetTTL () {
        return 1
      }
    })
    const instance = p.instance

    const subs = [{
      clientId: 'ttlTest',
      topic: 'hello',
      qos: 1
    }]
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'ttl test',
      qos: 1,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    instance.outgoingEnqueueCombi(subs, packet, async function enqueued (err, saved) {
      t.assert.ifError(err)
      t.assert.deepEqual(saved, packet)
      await sleep(1)
      const offlineStream = instance.outgoingStream({ id: 'ttlTest' })
      for await (const offlinePacket of offlineStream) {
        t.assert.ok(!offlinePacket)
      }
      cleanUpPersistence(t, p)
      resolve()
    })
  })
  await executeTest
})

test('outgoingUpdate doesn\'t clear packet ttl', async t => {
  t.plan(3)
  db.flushall()
  const p = setUpPersistence(t, '1', {
    packetTTL () {
      return 1
    }
  })
  const instance = p.instance

  const client = {
    id: 'ttlTest'
  }
  const subs = [{
    clientId: client.id,
    topic: 'hello',
    qos: 1
  }]
  const packet = {
    cmd: 'publish',
    topic: 'hello',
    payload: 'ttl test',
    qos: 1,
    retain: false,
    brokerId: instance.broker.id,
    brokerCounter: 42,
    messageId: 123
  }

  await new Promise((resolve, reject) => {
    instance.outgoingEnqueueCombi(subs, packet, function enqueued (err, saved) {
      t.assert.ifError(err)
      t.assert.deepEqual(saved, packet)
      instance.outgoingUpdate(client, packet, async function updated () {
        await sleep(2)
        db.exists('packet:1:42', (_, exists) => {
          t.assert.ok(!exists, 'packet key should have expired')
          cleanUpPersistence(t, p)
          resolve()
        })
      })
    })
  })
})

test('multiple persistences', {
  timeout: 60 * 1000
}, async t => {
  t.plan(3)
  const executeTest = new Promise((resolve, reject) => {
    db.flushall()
    const p1 = setUpPersistence(t, '1')
    const p2 = setUpPersistence(t, '2')
    const instance = p1.instance
    const instance2 = p2.instance

    const client = { id: 'multipleTest' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'hello/#',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }]

    let gotSubs = false
    let addedSubs = false

    function close () {
      if (gotSubs && addedSubs) {
        cleanUpPersistence(t, p1)
        cleanUpPersistence(t, p2)
        resolve()
      }
    }

    instance2._waitFor(client, true, 'hello', () => {
      instance2.subscriptionsByTopic('hello', (err, resubs) => {
        t.assert.ok(!err, 'subs by topic no error')
        t.assert.deepEqual(resubs, [{
          clientId: client.id,
          topic: 'hello/#',
          qos: 1,
          rh: undefined,
          rap: undefined,
          nl: undefined
        }, {
          clientId: client.id,
          topic: 'hello',
          qos: 1,
          rh: undefined,
          rap: undefined,
          nl: undefined
        }], 'received correct subs')
        gotSubs = true
        close()
      })
    })

    let ready = false
    let ready2 = false

    function addSubs () {
      if (ready && ready2) {
        instance.addSubscriptions(client, subs, err => {
          t.assert.ok(!err, 'add subs no error')
          addedSubs = true
          close()
        })
      }
    }

    instance.on('ready', () => {
      ready = true
      addSubs()
    })

    instance2.on('ready', () => {
      ready2 = true
      addSubs()
    })
  })
  await executeTest
})

test('unknown cache key', async t => {
  t.plan(2)
  const executeTest = new Promise((resolve, reject) => {
    db.flushall()
    const p = setUpPersistence(t, '1')
    const instance = p.instance
    const client = { id: 'unknown_pubrec' }

    // packet with no brokerId
    const packet = {
      cmd: 'pubrec',
      topic: 'hello',
      qos: 2,
      retain: false
    }

    instance.on('ready', () => {
      instance.outgoingUpdate(client, packet, (err, client, packet) => {
        t.assert.ok(err, 'error received')
        t.assert.equal(err.message, 'unknown key', 'Received unknown PUBREC')
        cleanUpPersistence(t, p)
        resolve()
      })
    })
  })
  await executeTest
})

test('wills table de-duplicate', async t => {
  t.plan(3)
  const executeTest = new Promise((resolve, reject) => {
    db.flushall()
    const p = setUpPersistence(t, '1')
    const instance = p.instance
    const client = { id: 'willsTest' }

    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'willsTest',
      qos: 1,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: 123
    }

    instance.putWill(client, packet, err => {
      t.assert.ok(!err, 'putWill #1 no error')
      instance.putWill(client, packet, err => {
        t.assert.ok(!err, 'putWill #2 no error')
        let willCount = 0
        const wills = instance.streamWill()
        wills.on('data', (chunk) => {
          willCount++
        })
        wills.on('end', () => {
          t.assert.equal(willCount, 1, 'should only be one will')
          cleanUpPersistence(t, p)
          resolve()
        })
      })
    })
  })
  await executeTest
})

test('matchRetained properly retrieves retained packets', async t => {
  t.plan(2)
  const executeTest = new Promise((resolve, reject) => {
    db.flushall()
    const p = setUpPersistence(t, '1')
    const instance = p.instance

    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'test',
      qos: 1,
      retain: true,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: 123
    }

    const qlobber = new QlobberTrue(persistence.forTesting.qlobberOpts)
    qlobber.add(packet.topic)

    instance.on('ready', () => {
      instance.storeRetained(packet, err => {
        t.assert.ok(!err, 'no error on storeRetained')
        persistence.forTesting.matchRetained(db, qlobber, false).next().then(retained => {
          t.assert.deepEqual(retained.value, packet, 'retained packet matches original packet')
          cleanUpPersistence(t, p)
          resolve()
        })
      })
    })
  })
  await executeTest
})

// clients will keep on running after the test
sleep(10).then(() => process.exit(0))
