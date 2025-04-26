const test = require('node:test')
const persistence = require('../persistence.js')
const Redis = require('ioredis')
const mqemitterRedis = require('mqemitter-redis')

// promisified versions of the instance methods
// to avoid deep callbacks while testing

async function outgoingEnqueueCombi (instance, subs, packet) {
  return new Promise((resolve, reject) => {
    instance.outgoingEnqueueCombi(subs, packet, err => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
}

async function outgoingUpdate (instance, client, packet) {
  return new Promise((resolve, reject) => {
    instance.outgoingUpdate(client, packet, (err, reclient, repacket) => {
      if (err) {
        reject(err)
      } else {
        resolve({ reclient, repacket })
      }
    })
  })
}

async function subscriptionsByTopic (instance, topic) {
  return new Promise((resolve, reject) => {
    instance.subscriptionsByTopic(topic, (err, resubs) => {
      if (err) {
        reject(err)
      } else {
        resolve(resubs)
      }
    })
  })
}

async function addSubscriptions (instance, client, subs) {
  return new Promise((resolve, reject) => {
    instance.addSubscriptions(client, subs, (err, reClient) => {
      if (err) {
        reject(err)
      } else {
        resolve(reClient)
      }
    })
  })
}

async function putWill (instance, client, packet) {
  return new Promise((resolve, reject) => {
    instance.putWill(client, packet, (err, reClient) => {
      if (err) {
        reject(err)
      } else {
        resolve(reClient)
      }
    })
  })
}

// helpers
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

async function createDB () {
  const db = new Redis()
  await waitForEvent(db, 'connect')
  await db.flushall()
  return db
}

async function cleanDB () {
  const db = await createDB()
  await db.disconnect()
}

// testing starts here
async function doTest () {
  test('external Redis conn', async t => {
    t.plan(1)
    const conn = await createDB(t)
    const p = setUpPersistence(t, '1', {
      conn
    })
    await waitForEvent(p.instance, 'ready')
    t.assert.ok(true, 'instance ready')
    t.diagnostic('instance ready')
    conn.disconnect()
    t.diagnostic('redis disconnected')
    cleanUpPersistence(t, p)
  })

  test('packet ttl', async t => {
    t.plan(1)
    await cleanDB()

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
    await outgoingEnqueueCombi(instance, subs, packet)
    await sleep(2)
    const offlineStream = instance.outgoingStream({ id: 'ttlTest' })
    for await (const offlinePacket of offlineStream) {
      t.assert.ok(!offlinePacket)
    }
    cleanUpPersistence(t, p)
  })

  test('outgoingUpdate doesn\'t clear packet ttl', async t => {
    t.plan(1)
    const db = await createDB()
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

    await outgoingEnqueueCombi(instance, subs, packet)
    await outgoingUpdate(instance, client, packet)
    await sleep(2)
    const exists = await db.exists('packet:1:42')
    t.assert.ok(!exists, 'packet key should have expired')
    cleanUpPersistence(t, p)
    db.disconnect()
  })

  test('multiple persistences', async t => {
    t.plan(1)
    await cleanDB()
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

    await addSubscriptions(instance, client, subs)
    const resubs = await subscriptionsByTopic(instance2, 'hello')
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

    cleanUpPersistence(t, p1)
    cleanUpPersistence(t, p2)
  })

  test('unknown cache key', async t => {
    t.plan(2)
    await cleanDB()

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

    try {
      await outgoingUpdate(instance, client, packet)
    } catch (err) {
      t.assert.ok(err, 'error received')
      t.assert.equal(err.message, 'unknown key', 'Received unknown PUBREC')
    }
    cleanUpPersistence(t, p)
  })

  test('wills table de-duplicate', async t => {
    t.plan(1)
    await cleanDB()

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

    await putWill(instance, client, packet)
    await putWill(instance, client, packet)
    const wills = await instance.streamWill().toArray()
    t.assert.equal(wills.length, 1, 'should only be one will')
    cleanUpPersistence(t, p)
  })
}
doTest()
