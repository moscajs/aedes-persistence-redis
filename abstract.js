const { Readable } = require('node:stream')
const { promisify } = require('node:util')
const Packet = require('aedes-packet')

// promisified versions of the instance methods
// to avoid deep callbacks while testing
function storeRetained (instance, packet) {
  return new Promise((resolve, reject) => {
    instance.storeRetained(packet, err => {
      if (err) {
        reject(err)
      } else {
        resolve()
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

async function removeSubscriptions (instance, client, subs) {
  return new Promise((resolve, reject) => {
    instance.removeSubscriptions(client, subs, (err, reClient) => {
      if (err) {
        reject(err)
      } else {
        resolve(reClient)
      }
    })
  })
}

async function subscriptionsByClient (instance, client) {
  return new Promise((resolve, reject) => {
    instance.subscriptionsByClient(client, (err, resubs, reClient) => {
      if (err) {
        reject(err)
      } else {
        resolve({ resubs, reClient })
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

async function cleanSubscriptions (instance, client) {
  return new Promise((resolve, reject) => {
    instance.cleanSubscriptions(client, (err) => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
}

async function countOffline (instance) {
  return new Promise((resolve, reject) => {
    instance.countOffline((err, subsCount, clientsCount) => {
      if (err) {
        reject(err)
      } else {
        resolve({ subsCount, clientsCount })
      }
    })
  })
}

async function outgoingEnqueue (instance, sub, packet) {
  return new Promise((resolve, reject) => {
    instance.outgoingEnqueue(sub, packet, err => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
}

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

async function outgoingClearMessageId (instance, client, packet) {
  return new Promise((resolve, reject) => {
    instance.outgoingClearMessageId(client, packet, (err, repacket) => {
      if (err) {
        reject(err)
      } else {
        resolve(repacket)
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

async function incomingStorePacket (instance, client, packet) {
  return new Promise((resolve, reject) => {
    instance.incomingStorePacket(client, packet, err => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })
  })
}
async function incomingGetPacket (instance, client, packet) {
  return new Promise((resolve, reject) => {
    instance.incomingGetPacket(client, packet, (err, retrieved) => {
      if (err) {
        reject(err)
      } else {
        resolve(retrieved)
      }
    })
  })
}

async function incomingDelPacket (instance, client, packet) {
  return new Promise((resolve, reject) => {
    instance.incomingDelPacket(client, packet, err => {
      if (err) {
        reject(err)
      } else {
        resolve()
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

async function getWill (instance, client) {
  return new Promise((resolve, reject) => {
    instance.getWill(client, (err, packet, reClient) => {
      if (err) {
        reject(err)
      } else {
        resolve({ packet, reClient })
      }
    })
  })
}

async function delWill (instance, client) {
  return new Promise((resolve, reject) => {
    instance.delWill(client, (err, packet, reClient) => {
      if (err) {
        reject(err)
      } else {
        resolve({ packet, reClient })
      }
    })
  })
}
// end of promisified versions of instance methods

// helper functions
function waitForEvent (obj, resolveEvt) {
  return new Promise((resolve, reject) => {
    obj.once(resolveEvt, () => {
      resolve()
    })
    obj.once('error', reject)
  })
}

async function doCleanup (t, instance) {
  const instanceDestroy = promisify(instance.destroy).bind(instance)
  await instanceDestroy()
  t.diagnostic('instance cleaned up')
}

// legacy third party streams are typically not iterable
function iterableStream (stream) {
  if (typeof stream[Symbol.asyncIterator] !== 'function') {
    return new Readable({ objectMode: true }).wrap(stream)
  }
  return stream
}
// end of legacy third party streams support

function outgoingStream (instance, client) {
  return iterableStream(instance.outgoingStream(client))
}

async function getArrayFromStream (stream) {
  const list = []
  for await (const item of iterableStream(stream)) {
    list.push(item)
  }
  return list
}

async function storeRetainedPacket (instance, opts = {}) {
  const packet = {
    cmd: 'publish',
    id: instance.broker.id,
    topic: opts.topic || 'hello/world',
    payload: opts.payload || Buffer.from('muahah'),
    qos: 0,
    retain: true
  }
  await storeRetained(instance, packet)
  return packet
}

async function enqueueAndUpdate (t, instance, client, sub, packet, messageId) {
  await outgoingEnqueueCombi(instance, [sub], packet)
  const updated = new Packet(packet)
  updated.messageId = messageId

  const { reclient, repacket } = await outgoingUpdate(instance, client, updated)
  t.assert.equal(reclient, client, 'client matches')
  t.assert.equal(repacket, updated, 'packet matches')
  return repacket
}

function testPacket (t, packet, expected) {
  if (packet.messageId === null) packet.messageId = undefined
  t.assert.equal(packet.messageId, undefined, 'should have an unassigned messageId in queue')
  // deepLooseEqual?
  t.assert.deepEqual(structuredClone(packet), expected, 'must return the packet')
}

function deClassed (obj) {
  return Object.assign({}, obj)
}

// start of abstractPersistence
function abstractPersistence (opts) {
  const test = opts.test
  const _persistence = opts.persistence
  const waitForReady = opts.waitForReady

  // requiring it here so it will not error for modules
  // not using the default emitter
  const buildEmitter = opts.buildEmitter || require('mqemitter')

  async function persistence (t) {
    const mq = buildEmitter()
    const broker = {
      id: 'broker-42',
      mq,
      publish: mq.emit.bind(mq),
      subscribe: mq.on.bind(mq),
      unsubscribe: mq.removeListener.bind(mq),
      counter: 0
    }

    const instance = await _persistence()
    if (instance) {
      // Wait for ready event, if applicable, to ensure the persistence isn't
      // destroyed while it's still being set up.
      // https://github.com/mcollina/aedes-persistence-redis/issues/41
      if (waitForReady) {
        await waitForEvent(instance, 'ready')
      }
      instance.broker = broker
      t.diagnostic('instance created')
      return instance
    }
    throw new Error('no instance')
  }

  async function matchRetainedWithPattern (t, pattern) {
    const instance = await persistence(t)
    const packet = await storeRetainedPacket(instance)
    let stream
    if (Array.isArray(pattern)) {
      stream = instance.createRetainedStreamCombi(pattern)
    } else {
      stream = instance.createRetainedStream(pattern)
    }
    t.diagnostic('created stream')
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [packet], 'must return the packet')
    t.diagnostic('stream was ok')
    await doCleanup(t, instance)
  }

  // testing starts here
  test('store and look up retained messages', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, 'hello/world')
  })

  test('look up retained messages with a # pattern', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, '#')
  })

  test('look up retained messages with a hello/world/# pattern', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, 'hello/world/#')
  })

  test('look up retained messages with a + pattern', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, 'hello/+')
  })

  test('look up retained messages with multiple patterns', async t => {
    t.plan(1)
    await matchRetainedWithPattern(t, ['hello/+', 'other/hello'])
  })

  test('store multiple retained messages in order', async (t) => {
    t.plan(1000)
    const instance = await persistence(t)
    const totalMessages = 1000

    const retained = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: true
    }

    for (let i = 0; i < totalMessages; i++) {
      const packet = new Packet(retained, instance.broker)
      await storeRetainedPacket(instance, packet)
      t.assert.equal(packet.brokerCounter, i + 1, 'packet stored in order')
    }
    await doCleanup(t, instance)
  })

  test('remove retained message', async (t) => {
    t.plan(1)
    const instance = await persistence(t)
    await storeRetainedPacket(instance, {})
    await storeRetainedPacket(instance, {
      payload: Buffer.alloc(0)
    })
    const stream = instance.createRetainedStream('#')
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [], 'must return an empty list')
    await doCleanup(t, instance)
  })

  test('storing twice a retained message should keep only the last', async (t) => {
    t.plan(1)
    const instance = await persistence(t)
    await storeRetainedPacket(instance, {})
    const packet = await storeRetainedPacket(instance, {
      payload: Buffer.from('ahah')
    })
    const stream = instance.createRetainedStream('#')
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [packet], 'must return the last packet')
    await doCleanup(t, instance)
  })

  test('Create a new packet while storing a retained message', async (t) => {
    t.plan(1)
    const instance = await persistence(t)
    const packet = {
      cmd: 'publish',
      id: instance.broker.id,
      topic: opts.topic || 'hello/world',
      payload: opts.payload || Buffer.from('muahah'),
      qos: 0,
      retain: true
    }
    const newPacket = Object.assign({}, packet)

    await storeRetained(instance, packet)
    // packet reference change to check if a new packet is stored always
    packet.retain = false
    const stream = instance.createRetainedStream('#')
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [newPacket], 'must return the last packet')
    await doCleanup(t, instance)
  })

  test('store and look up subscriptions by client', async (t) => {
    t.plan(3)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'noqos',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reClient = await addSubscriptions(instance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const { resubs, reClient: reClient2 } = await subscriptionsByClient(instance, client)
    t.assert.equal(reClient2, client, 'client must be the same')
    t.assert.deepEqual(resubs, subs)
    await doCleanup(t, instance)
  })

  test('remove subscriptions by client', async (t) => {
    t.plan(4)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reclient1 = await addSubscriptions(instance, client, subs)
    t.assert.equal(reclient1, client, 'client must be the same')
    const reClient2 = await removeSubscriptions(instance, client, ['hello'])
    t.assert.equal(reClient2, client, 'client must be the same')
    const { resubs, reClient } = await subscriptionsByClient(instance, client)
    t.assert.equal(reClient, client, 'client must be the same')
    t.assert.deepEqual(resubs, [{
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])
    await doCleanup(t, instance)
  })

  test('store and look up subscriptions by topic', async (t) => {
    t.plan(2)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reclient = await addSubscriptions(instance, client, subs)
    t.assert.equal(reclient, client, 'client must be the same')
    const resubs = await subscriptionsByTopic(instance, 'hello')
    t.assert.deepEqual(resubs, [{
      clientId: client.id,
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      clientId: client.id,
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])
    await doCleanup(t, instance)
  })

  test('get client list after subscriptions', async (t) => {
    t.plan(1)
    const instance = await persistence(t)
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]

    await addSubscriptions(instance, client1, subs)
    await addSubscriptions(instance, client2, subs)
    const stream = instance.getClientList(subs[0].topic)
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [client1.id, client2.id])
    await doCleanup(t, instance)
  })

  test('get client list after an unsubscribe', async (t) => {
    t.plan(1)
    const instance = await persistence(t)
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]
    await addSubscriptions(instance, client1, subs)
    await addSubscriptions(instance, client2, subs)
    await removeSubscriptions(instance, client2, [subs[0].topic])
    const stream = instance.getClientList(subs[0].topic)
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [client1.id])
    await doCleanup(t, instance)
  })

  test('get subscriptions list after an unsubscribe', async (t) => {
    t.plan(1)
    const instance = await persistence(t)
    const client1 = { id: 'abcde' }
    const client2 = { id: 'efghi' }
    const subs = [{
      topic: 'helloagain',
      qos: 1
    }]
    await addSubscriptions(instance, client1, subs)
    await addSubscriptions(instance, client2, subs)
    await removeSubscriptions(instance, client2, [subs[0].topic])
    const clients = await subscriptionsByTopic(instance, subs[0].topic)
    t.assert.deepEqual(clients[0].clientId, client1.id)
    await doCleanup(t, instance)
  })

  test('QoS 0 subscriptions, restored but not matched', async (t) => {
    t.plan(2)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'matteo',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    await addSubscriptions(instance, client, subs)
    const { resubs } = await subscriptionsByClient(instance, client)
    t.assert.deepEqual(resubs, subs)
    const resubs2 = await subscriptionsByTopic(instance, 'hello')
    t.assert.deepEqual(resubs2, [{
      clientId: client.id,
      topic: 'hello/#',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])
    await doCleanup(t, instance)
  })

  test('clean subscriptions', async (t) => {
    t.plan(4)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }]

    await addSubscriptions(instance, client, subs)
    await cleanSubscriptions(instance, client)
    const resubs = await subscriptionsByTopic(instance, 'hello')
    t.assert.deepEqual(resubs, [], 'no subscriptions')
    const { resubs: resubs2 } = await subscriptionsByClient(instance, client)
    t.assert.deepEqual(resubs2, null, 'no subscriptions')
    const { subsCount, clientsCount } = await countOffline(instance)
    t.assert.equal(subsCount, 0, 'no subscriptions added')
    t.assert.equal(clientsCount, 0, 'no clients added')
    await doCleanup(t, instance)
  })

  test('clean subscriptions with no active subscriptions', async (t) => {
    t.plan(4)
    const instance = await persistence(t)
    const client = { id: 'abcde' }

    await cleanSubscriptions(instance, client)
    const resubs = await subscriptionsByTopic(instance, 'hello')
    t.assert.deepEqual(resubs, [], 'no subscriptions')
    const { resubs: resubs2 } = await subscriptionsByClient(instance, client)
    t.assert.deepEqual(resubs2, null, 'no subscriptions')
    const { subsCount, clientsCount } = await countOffline(instance)
    t.assert.equal(subsCount, 0, 'no subscriptions added')
    t.assert.equal(clientsCount, 0, 'no clients added')
    await doCleanup(t, instance)
  })

  test('same topic, different QoS', async (t) => {
    t.plan(5)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }, {
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reClient = await addSubscriptions(instance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const { resubs } = await subscriptionsByClient(instance, client)
    t.assert.deepEqual(resubs, [{
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])

    const resubs2 = await subscriptionsByTopic(instance, 'hello')
    t.assert.deepEqual(resubs2, [{
      clientId: 'abcde',
      topic: 'hello',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }])

    const { subsCount, clientsCount } = await countOffline(instance)
    t.assert.equal(subsCount, 1, 'one subscription added')
    t.assert.equal(clientsCount, 1, 'one client added')
    await doCleanup(t, instance)
  })

  test('replace subscriptions', async (t) => {
    t.plan(25)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const topic = 'hello'
    const sub = { topic, rh: 0, rap: true, nl: false }
    const subByTopic = { clientId: client.id, topic, rh: 0, rap: true, nl: false }

    async function check (qos) {
      sub.qos = subByTopic.qos = qos
      const reClient = await addSubscriptions(instance, client, [sub])
      t.assert.equal(reClient, client, 'client must be the same')
      const { resubs } = await subscriptionsByClient(instance, client)
      t.assert.deepEqual(resubs, [sub])
      const subsForTopic = await subscriptionsByTopic(instance, topic)
      t.assert.deepEqual(subsForTopic, qos === 0 ? [] : [subByTopic])
      const { subsCount, clientsCount } = await countOffline(instance)
      if (qos === 0) {
        t.assert.equal(subsCount, 0, 'no subscriptions added')
      } else {
        t.assert.equal(subsCount, 1, 'one subscription added')
      }
      t.assert.equal(clientsCount, 1, 'one client added')
    }

    await check(0)
    await check(1)
    await check(2)
    await check(1)
    await check(0)
    await doCleanup(t, instance)
  })

  test('replace subscriptions in same call', async (t) => {
    t.plan(5)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [
      { topic, qos: 0, rh: 0, rap: true, nl: false },
      { topic, qos: 1, rh: 0, rap: true, nl: false },
      { topic, qos: 2, rh: 0, rap: true, nl: false },
      { topic, qos: 1, rh: 0, rap: true, nl: false },
      { topic, qos: 0, rh: 0, rap: true, nl: false }
    ]
    const reClient = await addSubscriptions(instance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const { resubs: subsForClient } = await subscriptionsByClient(instance, client)
    t.assert.deepEqual(subsForClient, [{ topic, qos: 0, rh: 0, rap: true, nl: false }])
    const subsForTopic = await subscriptionsByTopic(instance, topic)
    t.assert.deepEqual(subsForTopic, [])
    const { subsCount, clientsCount } = await countOffline(instance)
    t.assert.equal(subsCount, 0, 'no subscriptions added')
    t.assert.equal(clientsCount, 1, 'one client added')
    await doCleanup(t, instance)
  })

  test('store and count subscriptions', async (t) => {
    t.plan(11)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }, {
      topic: 'noqos',
      qos: 0
    }]

    const reclient = await addSubscriptions(instance, client, subs)
    t.assert.equal(reclient, client, 'client must be the same')
    const { subsCount, clientsCount } = await countOffline(instance)
    t.assert.equal(subsCount, 2, 'two subscriptions added')
    t.assert.equal(clientsCount, 1, 'one client added')
    await removeSubscriptions(instance, client, ['hello'])
    const { subsCount: subsCount2, clientsCount: clientsCount2 } = await countOffline(instance)
    t.assert.equal(subsCount2, 1, 'one subscription added')
    t.assert.equal(clientsCount2, 1, 'one client added')
    await removeSubscriptions(instance, client, ['matteo'])
    const { subsCount: subsCount3, clientsCount: clientsCount3 } = await countOffline(instance)
    t.assert.equal(subsCount3, 0, 'zero subscriptions added')
    t.assert.equal(clientsCount3, 1, 'one client added')
    await removeSubscriptions(instance, client, ['noqos'])
    const { subsCount: subsCount4, clientsCount: clientsCount4 } = await countOffline(instance)
    t.assert.equal(subsCount4, 0, 'zero subscriptions added')
    t.assert.equal(clientsCount4, 0, 'zero clients added')
    await removeSubscriptions(instance, client, ['noqos'])
    const { subsCount: subsCount5, clientsCount: clientsCount5 } = await countOffline(instance)
    t.assert.equal(subsCount5, 0, 'zero subscriptions added')
    t.assert.equal(clientsCount5, 0, 'zero clients added')
    await doCleanup(t, instance)
  })

  test('count subscriptions with two clients', async (t) => {
    t.plan(26)
    const instance = await persistence(t)
    const client1 = { id: 'abcde' }
    const client2 = { id: 'fghij' }
    const subs = [{
      topic: 'hello',
      qos: 1
    }, {
      topic: 'matteo',
      qos: 1
    }, {
      topic: 'noqos',
      qos: 0
    }]

    async function remove (client, subs, expectedSubs, expectedClients) {
      const reClient = await removeSubscriptions(instance, client, subs)
      t.assert.equal(reClient, client, 'client must be the same')
      const { subsCount, clientsCount } = await countOffline(instance)
      t.assert.equal(subsCount, expectedSubs, 'subscriptions added')
      t.assert.equal(clientsCount, expectedClients, 'clients added')
    }

    const reClient1 = await addSubscriptions(instance, client1, subs)
    t.assert.equal(reClient1, client1, 'client must be the same')
    const reClient2 = await addSubscriptions(instance, client2, subs)
    t.assert.equal(reClient2, client2, 'client must be the same')
    await remove(client1, ['foobar'], 4, 2)
    await remove(client1, ['hello'], 3, 2)
    await remove(client1, ['hello'], 3, 2)
    await remove(client1, ['matteo'], 2, 2)
    await remove(client1, ['noqos'], 2, 1)
    await remove(client2, ['hello'], 1, 1)
    await remove(client2, ['matteo'], 0, 1)
    await remove(client2, ['noqos'], 0, 0)
    await doCleanup(t, instance)
  })

  test('add duplicate subs to persistence for qos > 0', async (t) => {
    t.plan(3)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [{
      topic,
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reClient = await addSubscriptions(instance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const reClient2 = await addSubscriptions(instance, client, subs)
    t.assert.equal(reClient2, client, 'client must be the same')
    subs[0].clientId = client.id
    const subsForTopic = await subscriptionsByTopic(instance, topic)
    t.assert.deepEqual(subsForTopic, subs)
    await doCleanup(t, instance)
  })

  test('add duplicate subs to persistence for qos 0', async (t) => {
    t.plan(3)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const topic = 'hello'
    const subs = [{
      topic,
      qos: 0,
      rh: 0,
      rap: true,
      nl: false
    }]

    const reClient = await addSubscriptions(instance, client, subs)
    t.assert.equal(reClient, client, 'client must be the same')
    const reClient2 = await addSubscriptions(instance, client, subs)
    t.assert.equal(reClient2, client, 'client must be the same')
    const { resubs: subsForClient } = await subscriptionsByClient(instance, client)
    t.assert.deepEqual(subsForClient, subs)
    await doCleanup(t, instance)
  })

  test('get topic list after concurrent subscriptions of a client', async (t) => {
    t.plan(3)
    const instance = await persistence(t)
    const client = { id: 'abcde' }
    const subs1 = [{
      topic: 'hello1',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]
    const subs2 = [{
      topic: 'hello2',
      qos: 1,
      rh: 0,
      rap: true,
      nl: false
    }]
    let calls = 2

    await new Promise((resolve, reject) => {
      async function done () {
        if (!--calls) {
          const { resubs } = await subscriptionsByClient(instance, client)
          resubs.sort((a, b) => a.topic.localeCompare(b.topic, 'en'))
          t.assert.deepEqual(resubs, [subs1[0], subs2[0]])
          await doCleanup(t, instance)
          resolve()
        }
      }

      instance.addSubscriptions(client, subs1, err => {
        t.assert.ok(!err, 'no error for hello1')
        done()
      })
      instance.addSubscriptions(client, subs2, err => {
        t.assert.ok(!err, 'no error for hello2')
        done()
      })
    })
  })

  test('add outgoing packet and stream it', async (t) => {
    t.plan(2)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    await outgoingEnqueue(instance, sub, packet)
    const stream = outgoingStream(instance, client)
    const list = await getArrayFromStream(stream)
    testPacket(t, list[0], expected)
    await doCleanup(t, instance)
  })

  test('add outgoing packet for multiple subs and stream to all', async (t) => {
    t.plan(4)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const sub2 = {
      clientId: 'fghih',
      topic: 'hello',
      qos: 1
    }
    const subs = [sub, sub2]
    const client = {
      id: sub.clientId
    }
    const client2 = {
      id: sub2.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    await outgoingEnqueueCombi(instance, subs, packet)
    const stream = outgoingStream(instance, client)
    const list = await getArrayFromStream(stream)
    testPacket(t, list[0], expected)

    const stream2 = outgoingStream(instance, client2)
    const list2 = await getArrayFromStream(stream2)
    testPacket(t, list2[0], expected)
    await doCleanup(t, instance)
  })

  test('add outgoing packet as a string and pump', async (t) => {
    t.plan(7)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 10
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('matteo'),
      qos: 1,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 50
    }
    const queue = []

    const updated1 = await enqueueAndUpdate(t, instance, client, sub, packet1, 42)
    const updated2 = await enqueueAndUpdate(t, instance, client, sub, packet2, 43)
    const stream = outgoingStream(instance, client)

    async function clearQueue (data) {
      const { repacket } = await outgoingUpdate(instance, client, data)
      t.diagnostic('packet received')
      queue.push(repacket)
    }

    const list = await getArrayFromStream(stream)
    for (const data of list) {
      await clearQueue(data)
    }
    t.assert.equal(queue.length, 2)
    t.assert.deepEqual(deClassed(queue[0]), deClassed(updated1))
    t.assert.deepEqual(deClassed(queue[1]), deClassed(updated2))
    await doCleanup(t, instance)
  })

  test('add outgoing packet as a string and stream', async (t) => {
    t.plan(2)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'world',
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: 'world',
      qos: 1,
      retain: false,
      dup: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    await outgoingEnqueueCombi(instance, [sub], packet)
    const stream = outgoingStream(instance, client)
    const list = await getArrayFromStream(stream)
    testPacket(t, list[0], expected)
    await doCleanup(t, instance)
  })

  test('add outgoing packet and stream it twice', async (t) => {
    t.plan(5)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde',
      topic: 'hello',
      qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: 4242
    }
    const expected = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      retain: false,
      dup: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: undefined
    }

    await outgoingEnqueueCombi(instance, [sub], packet)
    const stream = outgoingStream(instance, client)
    const list = await getArrayFromStream(stream)
    testPacket(t, list[0], expected)
    const stream2 = outgoingStream(instance, client)
    const list2 = await getArrayFromStream(stream2)
    testPacket(t, list2[0], expected)
    t.assert.notEqual(packet, expected, 'packet must be a different object')
    await doCleanup(t, instance)
  })

  test('add outgoing packet and update messageId', async (t) => {
    t.plan(5)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }

    const updated = await enqueueAndUpdate(t, instance, client, sub, packet, 42)
    updated.messageId = undefined
    const stream = outgoingStream(instance, client)
    const list = await getArrayFromStream(stream)
    list[0].messageId = undefined
    t.assert.notEqual(list[0], updated, 'must not be the same object')
    t.assert.deepEqual(deClassed(list[0]), deClassed(updated), 'must return the packet')
    t.assert.equal(list.length, 1, 'must return only one packet')
    await doCleanup(t, instance)
  })

  test('add 2 outgoing packet and clear messageId', async (t) => {
    t.plan(10)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('matteo'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 43
    }

    const updated1 = await enqueueAndUpdate(t, instance, client, sub, packet1, 42)
    const updated2 = await enqueueAndUpdate(t, instance, client, sub, packet2, 43)
    const pkt = await outgoingClearMessageId(instance, client, updated1)
    t.assert.deepEqual(pkt.messageId, 42, 'must have the same messageId')
    t.assert.deepEqual(pkt.payload.toString(), packet1.payload.toString(), 'must have original payload')
    t.assert.deepEqual(pkt.topic, packet1.topic, 'must have original topic')
    const stream = outgoingStream(instance, client)
    updated2.messageId = undefined
    const list = await getArrayFromStream(stream)
    list[0].messageId = undefined
    t.assert.notEqual(list[0], updated2, 'must not be the same object')
    t.assert.deepEqual(deClassed(list[0]), deClassed(updated2), 'must return the packet')
    t.assert.equal(list.length, 1, 'must return only one packet')
    await doCleanup(t, instance)
  })

  test('add many outgoing packets and clear messageIds', async (t) => {
    // t.plan() is called below after we know the high watermark
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 1,
      dup: false,
      length: 14,
      retain: false
    }

    // we just need a stream to figure out the high watermark
    const stream = (outgoingStream(instance, client))
    const total = stream.readableHighWaterMark * 2
    t.plan(total * 2)

    for (let i = 0; i < total; i++) {
      const p = new Packet(packet, instance.broker)
      p.messageId = i
      await outgoingEnqueue(instance, sub, p)
      await outgoingUpdate(instance, client, p)
    }

    let queued = 0
    for await (const p of (outgoingStream(instance, client))) {
      if (p) {
        queued++
      }
    }
    t.assert.equal(queued, total, `outgoing queue must hold ${total} items`)

    for await (const p of (outgoingStream(instance, client))) {
      const received = await outgoingClearMessageId(instance, client, p)
      t.assert.deepEqual(received, p, 'must return the packet')
    }

    let queued2 = 0
    for await (const p of (outgoingStream(instance, client))) {
      if (p) {
        queued2++
      }
    }
    t.assert.equal(queued2, 0, 'outgoing queue is empty')
    await doCleanup(t, instance)
  })

  test('update to publish w/ same messageId', async (t) => {
    t.plan(5)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet1 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42,
      messageId: 42
    }
    const packet2 = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 50,
      messageId: 42
    }

    await outgoingEnqueue(instance, sub, packet1)
    await outgoingEnqueue(instance, sub, packet2)
    await outgoingUpdate(instance, client, packet1)
    await outgoingUpdate(instance, client, packet2)
    const stream = outgoingStream(instance, client)
    const list = await getArrayFromStream(stream)
    t.assert.equal(list.length, 2, 'must have two items in queue')
    t.assert.equal(list[0].brokerCounter, packet1.brokerCounter, 'brokerCounter must match')
    t.assert.equal(list[0].messageId, packet1.messageId, 'messageId must match')
    t.assert.equal(list[1].brokerCounter, packet2.brokerCounter, 'brokerCounter must match')
    t.assert.equal(list[1].messageId, packet2.messageId, 'messageId must match')
    await doCleanup(t, instance)
  })

  test('update to pubrel', async (t) => {
    t.plan(3)
    const instance = await persistence(t)
    const sub = {
      clientId: 'abcde', topic: 'hello', qos: 1
    }
    const client = {
      id: sub.clientId
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      brokerId: instance.broker.id,
      brokerCounter: 42
    }

    await outgoingEnqueueCombi(instance, [sub], packet)
    const updated = new Packet(packet)
    updated.messageId = 42
    const { reclient, repacket } = await outgoingUpdate(instance, client, updated)
    t.assert.equal(reclient, client, 'client matches')
    t.assert.equal(repacket, updated, 'packet matches')

    const pubrel = {
      cmd: 'pubrel',
      messageId: updated.messageId
    }

    await outgoingUpdate(instance, client, pubrel)
    const stream = outgoingStream(instance, client)
    const list = await getArrayFromStream(stream)
    t.assert.deepEqual(list, [pubrel], 'must return the packet')
    await doCleanup(t, instance)
  })

  test('add incoming packet, get it, and clear with messageId', async (t) => {
    t.plan(3)
    const instance = await persistence(t)
    const client = {
      id: 'abcde'
    }
    const packet = {
      cmd: 'publish',
      topic: 'hello',
      payload: Buffer.from('world'),
      qos: 2,
      dup: false,
      length: 14,
      retain: false,
      messageId: 42
    }
    await incomingStorePacket(instance, client, packet)
    const retrieved = await incomingGetPacket(instance, client, {
      messageId: packet.messageId
    })
    // adjusting the objects so they match
    delete retrieved.brokerCounter
    delete retrieved.brokerId
    delete packet.length
    // strip the class identifier from the packet
    const result = structuredClone(retrieved)
    // Convert Uint8 to Buffer for comparison
    result.payload = Buffer.from(result.payload)
    t.assert.deepEqual(result, packet, 'retrieved packet must be deeply equal')
    t.assert.notEqual(retrieved, packet, 'retrieved packet must not be the same object')
    await incomingDelPacket(instance, client, retrieved)

    try {
      await incomingGetPacket(instance, client, {
        messageId: packet.messageId
      })
      t.assert.ok(false, 'must error')
    } catch (err) {
      t.assert.ok(err, 'must error')
      await doCleanup(t, instance)
    }
  })

  test('store, fetch and delete will message', async (t) => {
    t.plan(7)
    const instance = await persistence(t)
    const client = {
      id: '12345'
    }
    const expected = {
      topic: 'hello/died',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const c = await putWill(instance, client, expected)
    t.assert.equal(c, client, 'client matches')
    const { packet: p1, reClient: c1 } = await getWill(instance, client)
    t.assert.deepEqual(p1, expected, 'will matches')
    t.assert.equal(c1, client, 'client matches')
    client.brokerId = p1.brokerId
    const { packet: p2, reClient: c2 } = await delWill(instance, client)
    t.assert.deepEqual(p2, expected, 'will matches')
    t.assert.equal(c2, client, 'client matches')
    const { packet: p3, reClient: c3 } = await getWill(instance, client)
    t.assert.ok(!p3, 'no will after del')
    t.assert.equal(c3, client, 'client matches')
    await doCleanup(t, instance)
  })

  test('stream all will messages', async (t) => {
    t.plan(3)
    const instance = await persistence(t)
    const client = {
      id: '12345',
      brokerId: instance.broker.id
    }
    const toWrite = {
      topic: 'hello/died',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const expected = {
      clientId: client.id,
      brokerId: instance.broker.id,
      topic: 'hello/died',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const c = await putWill(instance, client, toWrite)
    t.assert.equal(c, client, 'client matches')
    const stream = iterableStream(instance.streamWill())
    const list = await getArrayFromStream(stream)
    t.assert.equal(list.length, 1, 'must return only one packet')
    t.assert.deepEqual(list[0], expected, 'packet matches')
    await delWill(instance, client)
    await doCleanup(t, instance)
  })

  test('stream all will message for unknown brokers', async (t) => {
    t.plan(4)
    const instance = await persistence(t)
    const originalId = instance.broker.id
    const client = {
      id: '42',
      brokerId: instance.broker.id
    }
    const anotherClient = {
      id: '24',
      brokerId: instance.broker.id
    }
    const toWrite1 = {
      topic: 'hello/died42',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }
    const toWrite2 = {
      topic: 'hello/died24',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }
    const expected = {
      clientId: client.id,
      brokerId: originalId,
      topic: 'hello/died42',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const c = await putWill(instance, client, toWrite1)
    t.assert.equal(c, client, 'client matches')
    instance.broker.id = 'anotherBroker'
    const c2 = await putWill(instance, anotherClient, toWrite2)
    t.assert.equal(c2, anotherClient, 'client matches')
    const stream = iterableStream(instance.streamWill({
      anotherBroker: Date.now()
    }))
    const list = await getArrayFromStream(stream)
    t.assert.equal(list.length, 1, 'must return only one packet')
    t.assert.deepEqual(list[0], expected, 'packet matches')
    await delWill(instance, client)
    await doCleanup(t, instance)
  })

  test('delete wills from dead brokers', async (t) => {
    const instance = await persistence(t)
    const client = {
      id: '42'
    }

    const toWrite1 = {
      topic: 'hello/died42',
      payload: Buffer.from('muahahha'),
      qos: 0,
      retain: true
    }

    const c = await putWill(instance, client, toWrite1)
    t.assert.equal(c, client, 'client matches')
    instance.broker.id = 'anotherBroker'
    client.brokerId = instance.broker.id
    await delWill(instance, client)
    await doCleanup(t, instance)
  })

  test('do not error if unkown messageId in outoingClearMessageId', async (t) => {
    const instance = await persistence(t)
    const client = {
      id: 'abc-123'
    }

    await outgoingClearMessageId(instance, client, 42)
    await doCleanup(t, instance)
  })
}

module.exports = abstractPersistence
