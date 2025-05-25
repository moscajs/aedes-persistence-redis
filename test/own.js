const test = require('node:test')
const persistence = require('../persistence.js')
const Redis = require('ioredis')
const mqemitterRedis = require('mqemitter-redis')
const { PromisifiedPersistence } = require('aedes-persistence/promisified.js')
const { once } = require('node:events')

// helpers
function sleep (sec) {
  return new Promise(resolve => setTimeout(resolve, sec * 1000))
}

async function setUpPersistence (t, id, persistenceOpts) {
  const emitter = mqemitterRedis()
  const instance = persistence(persistenceOpts)
  instance.broker = toBroker(id, emitter)
  if (!instance.ready) {
    await once(instance, 'ready')
  }
  t.diagnostic(`instance ${id} created`)
  const p = new PromisifiedPersistence(instance)
  return { instance: p, emitter, id }
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
  await once(db, 'connect')
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
    const p = await setUpPersistence(t, '1', {
      conn
    })
    conn.disconnect()
    t.assert.ok(true, 'redis disconnected')
    t.diagnostic('redis disconnected')
    cleanUpPersistence(t, p)
  })

  test('packet ttl', async t => {
    t.plan(1)
    await cleanDB()

    const p = await setUpPersistence(t, '1', {
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
    await instance.outgoingEnqueueCombi(subs, packet)
    await sleep(2)
    const packets = await instance.outgoingStream({ id: 'ttlTest' }).toArray()
    const noPacket = (packets === undefined) || (packets[0] === undefined)
    t.assert.ok(noPacket, 'packet is gone')
    cleanUpPersistence(t, p)
  })

  test('outgoingUpdate doesn\'t clear packet ttl', async t => {
    t.plan(1)
    const db = await createDB()
    const p = await setUpPersistence(t, '1', {
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

    await instance.outgoingEnqueueCombi(subs, packet)
    await instance.outgoingUpdate(client, packet)
    await sleep(2)
    const exists = await db.exists('packet:1:42')
    t.assert.ok(!exists, 'packet key should have expired')
    cleanUpPersistence(t, p)
    db.disconnect()
  })

  test('multiple persistences', async t => {
    t.plan(1)
    await cleanDB()
    const p1 = await setUpPersistence(t, '1')
    const p2 = await setUpPersistence(t, '2')
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

    const expected = [{
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
    }]

    await instance.addSubscriptions(client, subs)
    await sleep(2)
    const resubs = await instance2.subscriptionsByTopic('hello')
    t.assert.deepEqual(resubs, expected, 'received correct subs')

    cleanUpPersistence(t, p1)
    cleanUpPersistence(t, p2)
  })

  test('unknown cache key', async t => {
    t.plan(2)
    await cleanDB()

    const p = await setUpPersistence(t, '1')
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
      await instance.outgoingUpdate(client, packet)
    } catch (err) {
      t.assert.ok(err, 'error received')
      t.assert.equal(err.message, 'unknown key', 'Received unknown PUBREC')
    }
    cleanUpPersistence(t, p)
  })

  test('wills table de-duplicate', async t => {
    t.plan(1)
    await cleanDB()

    const p = await setUpPersistence(t, '1')
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

    await instance.putWill(client, packet)
    await instance.putWill(client, packet)
    const wills = await instance.streamWill().toArray()
    t.assert.equal(wills.length, 1, 'should only be one will')
    cleanUpPersistence(t, p)
  })
}
doTest()
