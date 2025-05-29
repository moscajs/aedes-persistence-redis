'use strict'
const Redis = require('ioredis')
const msgpack = require('msgpack-lite')
const Packet = require('aedes-persistence').Packet
const BroadcastPersistence = require('aedes-persistence/broadcastPersistence.js')
const HLRU = require('hashlru')
const { QlobberTrue } = require('qlobber')
const QlobberSub = require('qlobber/aedes/qlobber-sub')
const QLOBBER_OPTIONS = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#',
  match_empty_levels: true
}
const CLIENTKEY = 'client:'
const CLIENTSKEY = 'clients'
const WILLSKEY = 'will'
const WILLKEY = 'will:'
const RETAINEDKEY = 'retained'
// const ALL_RETAINEDKEYS = `${RETAINEDKEY}:*`
const OUTGOINGKEY = 'outgoing:'
const OUTGOINGIDKEY = 'outgoing-id:'
const INCOMINGKEY = 'incoming:'
const PACKETKEY = 'packet:'

function clientSubKey (clientId) {
  return `${CLIENTKEY}${encodeURIComponent(clientId)}`
}

function willKey (brokerId, clientId) {
  return `${WILLKEY}${brokerId}:${encodeURIComponent(clientId)}`
}

function outgoingKey (clientId) {
  return `${OUTGOINGKEY}${encodeURIComponent(clientId)}`
}

function outgoingByBrokerKey (clientId, brokerId, brokerCounter) {
  return `${outgoingKey(clientId)}:${brokerId}:${brokerCounter}`
}

function outgoingIdKey (clientId, messageId) {
  return `${OUTGOINGIDKEY}${encodeURIComponent(clientId)}:${messageId}`
}

function incomingKey (clientId, messageId) {
  return `${INCOMINGKEY}${encodeURIComponent(clientId)}:${messageId}`
}

function packetKey (brokerId, brokerCounter) {
  return `${PACKETKEY}${brokerId}:${brokerCounter}`
}

function retainedKey (topic) {
  return `${RETAINEDKEY}:${encodeURIComponent(topic)}`
}

function packetCountKey (brokerId, brokerCounter) {
  return `${PACKETKEY}${brokerId}:${brokerCounter}:offlineCount`
}

async function getDecodedValue (db, listKey, key) {
  const value = await db.getBuffer(key)
  const decoded = value ? msgpack.decode(value) : undefined
  if (!decoded) {
    db.lrem(listKey, 0, key)
  }
  return decoded
}

function keyToTopic (key) {
  return decodeURIComponent(key.split(':')[1])
}

function longestCommonPrefix (patterns) {
  if (!patterns || patterns.length === 0) {
    return ''
  }

  patterns.sort() // Sort the array lexicographically
  const first = patterns[0]
  const last = patterns.at(-1)
  let i = 0

  while (i < first.length && i < last.length && first[i] === last[i]) {
    i++
  }
  return first.substring(0, i)
}

function wildCardPosition (pattern) {
  // return the first position of a wildcard or -1 if one is found
  // if present wildcard_some is always the last character of the pattern
  const oneIndex = pattern.indexOf(QLOBBER_OPTIONS.wildcard_one)
  // we found oneIndex, so it must be the first one
  if (oneIndex !== -1) {
    return oneIndex
  }
  // check for one wildcard_some
  const someIndex = pattern.indexOf(QLOBBER_OPTIONS.wildcard_some)
  return someIndex
}

async function getRetainedValue (db, topic, hasClusters) {
  if (hasClusters === true) {
    return await db.getBuffer(retainedKey(topic))
  }
  return await db.hgetBuffer(RETAINEDKEY, topic)
}

async function * createWillStream (db, brokers, maxWills) {
  for (const key of await db.lrange(WILLSKEY, 0, maxWills)) {
    const result = await getDecodedValue(db, WILLSKEY, key)
    if (!brokers || !brokers[key.split(':')[1]]) {
      yield result
    }
  }
}

function * getClientIdFromEntries (entries) {
  for (const entry of entries) {
    yield entry.clientId
  }
}

// ioredis does not provide a scanBufferStream on a cluster so we need to do this ourselves
// scanBuffer only returns keys!
async function * clusterMatchRetained (cluster, prefix, qlobber, sentTopics) {
  const opts = { match: retainedKey(prefix) }
  const masterNodes = cluster.nodes('master')
  for (const node of masterNodes) {
    for await (const key of node.scanBufferStream(opts)) {
      if (key.length === 0) {
        // nothing found
        continue
      }
      const topic = keyToTopic(key.toString())
      if (!sentTopics.has(topic) && qlobber.test(topic)) {
        const value = await cluster.getBuffer(retainedKey(topic))
        if (value) {
          const packet = msgpack.decode(value)
          yield packet
        }
      }
    }
  }
}

// hscanBuffer always returns keys + values!
async function * singleMatchRetained (db, prefix, qlobber, sentTopics) {
  const stream = db.hscanBufferStream(RETAINEDKEY, { match: prefix })
  for await (const data of stream) {
    for (let i = 0; i < data.length; i += 2) {
      const key = data[i]
      const value = data[i + 1]
      const topic = key.toString()
      if (qlobber.test(topic)) {
        const packet = msgpack.decode(value)
        if (packet && !sentTopics.has(packet.topic)) {
          yield packet
        }
      }
    }
  }
}

async function * matchRetained (db, patterns, hasClusters) {
  const wildcards = []
  const qlobber = new QlobberTrue(QLOBBER_OPTIONS)
  const sentTopics = new Set()

  for (const p of patterns) {
    const wPos = wildCardPosition(p)
    if (wPos === -1) {
      // no wildcards
      const packet = await getRetainedValue(db, p, hasClusters)
      if (packet) {
        // track for which topics a packet has been sent to avoid sending packets
        // twice because of combo of no-wildcard and wildcard
        sentTopics.add(packet.topic)
        yield msgpack.decode(packet)
      }
    } else {
      // wildcard present
      qlobber.add(p)
      wildcards.push(p.substring(0, wPos - 1))
    }
  }

  if (wildcards.length) {
    const prefix = longestCommonPrefix(wildcards) + '*'
    const stream = hasClusters ? clusterMatchRetained(db, prefix, qlobber, sentTopics) : singleMatchRetained(db, prefix, qlobber, sentTopics)
    for await (const packet of stream) {
      yield packet
    }
  }
}

function subsForClient (subs) {
  const subKeys = Object.keys(subs)

  const result = []

  if (subKeys.length === 0) {
    return result
  }

  for (const subKey of subKeys) {
    if (subs[subKey].length === 1) { // version 8x fallback, QoS saved not encoded object
      result.push({
        topic: subKey,
        qos: Number.parseInt(subs[subKey])
      })
    } else {
      result.push(msgpack.decode(subs[subKey]))
    }
  }

  return result
}

function processKeysForClient (clientId, clientHash, trie) {
  const topics = Object.keys(clientHash)
  for (const topic of topics) {
    const sub = msgpack.decode(clientHash[topic])
    sub.clientId = clientId
    trie.add(topic, sub)
  }
}

async function updateWithClientData (db, messageIdCache, packetTTL, client, packet) {
  const clientListKey = outgoingKey(client.id)
  const messageIdKey = outgoingIdKey(client.id, packet.messageId)
  const pktKey = packetKey(packet.brokerId, packet.brokerCounter)

  const ttl = packetTTL(packet)
  if (packet.cmd && packet.cmd !== 'pubrel') { // qos=1
    messageIdCache.set(messageIdKey, pktKey)
    if (ttl > 0) {
      const result = await db.set(pktKey, msgpack.encode(packet), 'EX', ttl)
      if (result !== 'OK') {
        throw new Error('no such packet')
      }
      return
    }
    const result = await db.set(pktKey, msgpack.encode(packet))
    if (result !== 'OK') {
      throw new Error('no such packet')
    }
    return
  }

  // qos=2
  const clientUpdateKey = outgoingByBrokerKey(client.id, packet.brokerId, packet.brokerCounter)
  messageIdCache.set(messageIdKey, clientUpdateKey)

  const removed = await db.lrem(clientListKey, 0, pktKey)
  if (removed === 1) {
    await db.rpush(clientListKey, clientUpdateKey)
  }

  const encoded = msgpack.encode(packet)
  if (ttl > 0) {
    const result = await db.set(clientUpdateKey, encoded, 'EX', ttl)
    if (result !== 'OK') {
      throw new Error('no such packet')
    }
    return
  }
  const result = await db.set(clientUpdateKey, encoded)
  if (result !== 'OK') {
    throw new Error('no such packet')
  }
}

function augmentWithBrokerData (messageIdCache, client, packet) {
  const messageIdKey = outgoingIdKey(client.id, packet.messageId)

  const key = messageIdCache.get(messageIdKey)
  if (!key) {
    throw new Error('unknown key')
  }
  const tokens = key.split(':')
  packet.brokerId = tokens[tokens.length - 2]
  packet.brokerCounter = tokens[tokens.length - 1]
}

class AsyncRedisPersistence {
  #trie
  #maxSessionDelivery
  #maxWills
  #packetTTL
  #messageIdCache
  #db
  #hasClusters
  #broadcast
  #broker
  #destroyed

  constructor (opts = {}) {
    this.#trie = new QlobberSub(QLOBBER_OPTIONS)
    this.#destroyed = false
    this.#maxSessionDelivery = opts.maxSessionDelivery || 1000
    this.#maxWills = 10000
    this.#packetTTL = opts.packetTTL || (() => { return 0 })

    this.#messageIdCache = HLRU(100000)

    if (opts.cluster && Array.isArray(opts.cluster)) {
      this.#db = new Redis.Cluster(opts.cluster)
    } else {
      this.#db = opts.conn || new Redis(opts)
    }

    this.#hasClusters = !!opts.cluster
  }

  // broker getter for testing only
  get broker () {
    return this.#broker
  }

  /* private methods start with a # */

  async setup (broker) {
    this.#broker = broker
    this.#broadcast = new BroadcastPersistence(broker, this.#trie)
    const clientIds = await this.#db.smembers(CLIENTSKEY)
    for await (const clientId of clientIds) {
      const clientHash = await this.#db.hgetallBuffer(clientSubKey(clientId))
      processKeysForClient(clientId, clientHash, this.#trie)
    }
    await this.#broadcast.brokerSubscribe()
  }

  /**
   * When using clusters we store it using a compound key instead of an hash
   * to spread the load across the clusters. See issue #85.
   */
  async #storeRetainedCluster (packet) {
    if (packet.payload.length === 0) {
      await this.#db.del(retainedKey(packet.topic))
    } else {
      await this.#db.set(retainedKey(packet.topic), msgpack.encode(packet))
    }
  }

  async #storeRetained (packet) {
    if (packet.payload.length === 0) {
      await this.#db.hdel(RETAINEDKEY, packet.topic)
    } else {
      await this.#db.hset(RETAINEDKEY, packet.topic, msgpack.encode(packet))
    }
  }

  async storeRetained (packet) {
    if (this.#hasClusters) {
      await this.#storeRetainedCluster(packet)
    } else {
      await this.#storeRetained(packet)
    }
  }

  createRetainedStreamCombi (patterns) {
    return matchRetained(this.#db, patterns, this.#hasClusters)
  }

  createRetainedStream (pattern) {
    return this.createRetainedStreamCombi([pattern])
  }

  async addSubscriptions (client, subs) {
    const toStore = {}

    for (const sub of subs) {
      toStore[sub.topic] = msgpack.encode(sub)
    }

    await this.#db.sadd(CLIENTSKEY, client.id)
    await this.#db.hmsetBuffer(clientSubKey(client.id), toStore)
    await this.#broadcast.addedSubscriptions(client, subs)
  }

  async removeSubscriptions (client, subs) {
    const clientSK = clientSubKey(client.id)
    // Remove the subscriptions from the client's subscription hash
    await this.#db.hdel(clientSK, subs)
    // Check if the client still has any subscriptions
    const subCount = await this.#db.exists(clientSK)
    if (subCount === 0) {
      // If no subscriptions remain, clean up the client's data
      await this.#db.del(outgoingKey(client.id))
      await this.#db.srem(CLIENTSKEY, client.id)
    }
    await this.#broadcast.removedSubscriptions(client, subs)
  }

  async subscriptionsByClient (client) {
    const subs = await this.#db.hgetallBuffer(clientSubKey(client.id))
    return subsForClient(subs)
  }

  async countOffline () {
    const count = await this.#db.scard(CLIENTSKEY)
    const clientsCount = Number.parseInt(count) || 0
    const subsCount = this.#trie.subscriptionsCount
    return { subsCount, clientsCount }
  }

  async subscriptionsByTopic (topic) {
    return this.#trie.match(topic)
  }

  async cleanSubscriptions (client) {
    const subs = await this.subscriptionsByClient(client)
    if (subs.length > 0) {
      const remSubs = subs.map(sub => sub.topic)
      await this.removeSubscriptions(client, remSubs)
    }
  }

  async outgoingEnqueue (sub, packet) {
    await this.outgoingEnqueueCombi([sub], packet)
  }

  async outgoingEnqueueCombi (subs, packet) {
    if (!subs || subs.length === 0) {
      return
    }

    const pktKey = packetKey(packet.brokerId, packet.brokerCounter)
    const countKey = packetCountKey(packet.brokerId, packet.brokerCounter)
    const ttl = this.#packetTTL(packet)

    const encoded = msgpack.encode(new Packet(packet))

    if (this.#hasClusters) {
      // do not do this using `mset`, fails in clusters
      await this.#db.set(pktKey, encoded)
      await this.#db.set(countKey, subs.length)
    } else {
      await this.#db.mset(pktKey, encoded, countKey, subs.length)
    }

    if (ttl > 0) {
      await this.#db.expire(pktKey, ttl)
      await this.#db.expire(countKey, ttl)
    }

    for (const sub of subs) {
      const listKey = outgoingKey(sub.clientId)
      await this.#db.rpush(listKey, pktKey)
    }
  }

  async outgoingUpdate (client, packet) {
    if (!('brokerId' in packet && 'messageId' in packet)) {
      augmentWithBrokerData(this.#messageIdCache, client, packet)
    }
    await updateWithClientData(this.#db, this.#messageIdCache, this.#packetTTL, client, packet)
  }

  async outgoingClearMessageId (client, packet) {
    const clientListKey = outgoingKey(client.id)
    const messageIdKey = outgoingIdKey(client.id, packet.messageId)

    const clientKey = this.#messageIdCache.get(messageIdKey)
    this.#messageIdCache.remove(messageIdKey)

    if (!clientKey) {
      return
    }

    // TODO can be cached in case of wildcard deliveries
    const buf = await this.#db.getBuffer(clientKey)

    let origPacket
    let pktKey
    let countKey

    if (buf) {
      origPacket = msgpack.decode(buf)
      origPacket.messageId = packet.messageId

      pktKey = packetKey(origPacket.brokerId, origPacket.brokerCounter)
      countKey = packetCountKey(origPacket.brokerId, origPacket.brokerCounter)

      if (clientKey !== pktKey) { // qos=2
        await this.#db.del(clientKey)
      }
    }
    await this.#db.lrem(clientListKey, 0, pktKey)

    const remained = await this.#db.decr(countKey)
    if (remained === 0) {
      if (this.#hasClusters) {
        // do not remove multiple keys at once, fails in clusters
        await this.#db.del(pktKey)
        await this.#db.del(countKey)
      } else {
        await this.#db.del(pktKey, countKey)
      }
    }
    return origPacket
  }

  outgoingStream (client) {
    const clientListKey = outgoingKey(client.id)
    const db = this.#db
    const maxSessionDelivery = this.#maxSessionDelivery

    async function * lrangeResult () {
      for (const key of await db.lrange(clientListKey, 0, maxSessionDelivery)) {
        yield getDecodedValue(db, clientListKey, key)
      }
    }

    return lrangeResult()
  }

  async incomingStorePacket (client, packet) {
    const key = incomingKey(client.id, packet.messageId)
    const newp = new Packet(packet)
    newp.messageId = packet.messageId
    await this.#db.set(key, msgpack.encode(newp))
  }

  async incomingGetPacket (client, packet) {
    const key = incomingKey(client.id, packet.messageId)
    const buf = await this.#db.getBuffer(key)
    if (!buf) {
      throw new Error('no such packet')
    }
    return msgpack.decode(buf)
  }

  async incomingDelPacket (client, packet) {
    const key = incomingKey(client.id, packet.messageId)
    await this.#db.del(key)
  }

  async putWill (client, packet) {
    const key = willKey(this.#broker.id, client.id)
    packet.clientId = client.id
    packet.brokerId = this.#broker.id
    this.#db.lrem(WILLSKEY, 0, key) // Remove duplicates
    this.#db.rpush(WILLSKEY, key)
    await this.#db.setBuffer(key, msgpack.encode(packet))
  }

  async getWill (client) {
    const key = willKey(this.#broker.id, client.id)
    const packet = await this.#db.getBuffer(key)
    const result = packet ? msgpack.decode(packet) : null
    return result
  }

  async delWill (client) {
    const key = willKey(client.brokerId, client.id)
    this.#db.lrem(WILLSKEY, 0, key)
    const packet = await this.#db.getBuffer(key)
    const result = packet ? msgpack.decode(packet) : null
    await this.#db.del(key)
    return result
  }

  streamWill (brokers) {
    return createWillStream(this.#db, brokers, this.#maxWills)
  }

  getClientList (topic) {
    const entries = this.#trie.match(topic, topic)
    return getClientIdFromEntries(entries)
  }

  #buildAugment (listKey) {
    const that = this
    return function decodeAndAugment (key, enc, cb) {
      that.#db.getBuffer(key, function decodeMessage (err, result) {
        let decoded
        if (result) {
          decoded = msgpack.decode(result)
        }
        if (err || !decoded) {
          that.#db.lrem(listKey, 0, key)
        }
        cb(err, decoded)
      })
    }
  }

  async destroy (cb) {
    if (this.#destroyed) {
      throw new Error('destroyed called twice!')
    }
    this.#destroyed = true
    await this.#broadcast.brokerUnsubscribe()
    await this.#db.disconnect()
  }
}

module.exports = AsyncRedisPersistence
