'use strict'
const Redis = require('ioredis')
const msgpack = require('msgpack-lite')
const Packet = require('aedes-cached-persistence').Packet
const HLRU = require('hashlru')
const { QlobberTrue } = require('qlobber')
const qlobberOpts = {
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
const ALL_RETAINEDKEYS = `${RETAINEDKEY}:*`
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

async function getRetainedKeys (db, hasClusters) {
  if (hasClusters === true) {
    // Get keys of all the masters
    const masters = db.nodes('master')
    const keys = await Promise.all(
      masters.map((node) => node.keys(ALL_RETAINEDKEYS))
    )
    return keys.flat()
  }
  return await db.hkeys(RETAINEDKEY)
}

async function getRetainedValue (db, topic, hasClusters) {
  if (hasClusters === true) {
    return msgpack.decode(await db.getBuffer(retainedKey(topic)))
  }
  return msgpack.decode(await db.hgetBuffer(RETAINEDKEY, topic))
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

async function * matchRetained (db, qlobber, hasClusters) {
  const keys = await getRetainedKeys(db, hasClusters)
  for (const key of keys) {
    const topic = hasClusters ? decodeURIComponent(key.split(':')[1]) : key
    if (qlobber.test(topic)) {
      yield getRetainedValue(db, topic, hasClusters)
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

async function updateWithClientData (that, client, packet) {
  const clientListKey = outgoingKey(client.id)
  const messageIdKey = outgoingIdKey(client.id, packet.messageId)
  const pktKey = packetKey(packet.brokerId, packet.brokerCounter)

  const ttl = that.packetTTL(packet)
  if (packet.cmd && packet.cmd !== 'pubrel') { // qos=1
    that.messageIdCache.set(messageIdKey, pktKey)
    if (ttl > 0) {
      const result = await that._db.set(pktKey, msgpack.encode(packet), 'EX', ttl)
      if (result !== 'OK') {
        throw new Error('no such packet')
      }
      return
    }
    const result = await that._db.set(pktKey, msgpack.encode(packet))
    if (result !== 'OK') {
      throw new Error('no such packet')
    }
    return
  }

  // qos=2
  const clientUpdateKey = outgoingByBrokerKey(client.id, packet.brokerId, packet.brokerCounter)
  that.messageIdCache.set(messageIdKey, clientUpdateKey)

  const removed = await that._db.lrem(clientListKey, 0, pktKey)
  if (removed === 1) {
    await that._db.rpush(clientListKey, clientUpdateKey)
  }

  const encoded = msgpack.encode(packet)
  if (ttl > 0) {
    const result = await that._db.set(clientUpdateKey, encoded, 'EX', ttl)
    if (result !== 'OK') {
      throw new Error('no such packet')
    }
    return
  }
  const result = await that._db.set(clientUpdateKey, encoded)
  if (result !== 'OK') {
    throw new Error('no such packet')
  }
}

function augmentWithBrokerData (that, client, packet) {
  const messageIdKey = outgoingIdKey(client.id, packet.messageId)

  const key = that.messageIdCache.get(messageIdKey)
  if (!key) {
    throw new Error('unknown key')
  }
  const tokens = key.split(':')
  packet.brokerId = tokens[tokens.length - 2]
  packet.brokerCounter = tokens[tokens.length - 1]
}

class AsyncRedisPersistence {
  constructor (opts = {}) {
    this.maxSessionDelivery = opts.maxSessionDelivery || 1000
    this.maxWills = 10000
    this.packetTTL = opts.packetTTL || (() => { return 0 })

    this.messageIdCache = HLRU(100000)

    if (opts.cluster && Array.isArray(opts.cluster)) {
      this._db = new Redis.Cluster(opts.cluster)
    } else {
      this._db = opts.conn || new Redis(opts)
    }

    this.hasClusters = !!opts.cluster
  }
  /* private methods start with a # */

  async setup () {
    const clientIds = await this._db.smembers(CLIENTSKEY)
    for await (const clientId of clientIds) {
      const clientHash = await this._db.hgetallBuffer(clientSubKey(clientId))
      processKeysForClient(clientId, clientHash, this._trie)
    }
  }

  /**
   * When using clusters we store it using a compound key instead of an hash
   * to spread the load across the clusters. See issue #85.
   */
  async #storeRetainedCluster (packet) {
    if (packet.payload.length === 0) {
      await this._db.del(retainedKey(packet.topic))
    } else {
      await this._db.set(retainedKey(packet.topic), msgpack.encode(packet))
    }
  }

  async #storeRetained (packet) {
    if (packet.payload.length === 0) {
      await this._db.hdel(RETAINEDKEY, packet.topic)
    } else {
      await this._db.hset(RETAINEDKEY, packet.topic, msgpack.encode(packet))
    }
  }

  async storeRetained (packet) {
    if (this.hasClusters) {
      await this.#storeRetainedCluster(packet)
    } else {
      await this.#storeRetained(packet)
    }
  }

  createRetainedStreamCombi (patterns) {
    const qlobber = new QlobberTrue(qlobberOpts)

    for (const pattern of patterns) {
      qlobber.add(pattern)
    }

    return matchRetained(this._db, qlobber, this.hasClusters)
  }

  createRetainedStream (pattern) {
    return this.createRetainedStreamCombi([pattern])
  }

  async addSubscriptions (client, subs) {
    const toStore = {}

    for (const sub of subs) {
      toStore[sub.topic] = msgpack.encode(sub)
    }

    await this._db.sadd(CLIENTSKEY, client.id)
    await this._db.hmsetBuffer(clientSubKey(client.id), toStore)
  }

  async removeSubscriptions (client, subs) {
    const clientSK = clientSubKey(client.id)
    // Remove the subscriptions from the client's subscription hash
    await this._db.hdel(clientSK, subs)
    // Check if the client still has any subscriptions
    const subCount = await this._db.exists(clientSK)
    if (subCount === 0) {
      // If no subscriptions remain, clean up the client's data
      await this._db.del(outgoingKey(client.id))
      await this._db.srem(CLIENTSKEY, client.id)
    }
  }

  async subscriptionsByClient (client) {
    const subs = await this._db.hgetallBuffer(clientSubKey(client.id))
    return subsForClient(subs)
  }

  async countOffline () {
    const count = await this._db.scard(CLIENTSKEY)
    const clientsCount = Number.parseInt(count) || 0
    const subscriptionsCount = this._trie.subscriptionsCount
    return { subscriptionsCount, clientsCount }
  }

  async subscriptionsByTopic (topic) {
    return this._trie.match(topic)
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
    const ttl = this.packetTTL(packet)

    const encoded = msgpack.encode(new Packet(packet))

    if (this.hasClusters) {
      // do not do this using `mset`, fails in clusters
      await this._db.set(pktKey, encoded)
      await this._db.set(countKey, subs.length)
    } else {
      await this._db.mset(pktKey, encoded, countKey, subs.length)
    }

    if (ttl > 0) {
      await this._db.expire(pktKey, ttl)
      await this._db.expire(countKey, ttl)
    }

    for (const sub of subs) {
      const listKey = outgoingKey(sub.clientId)
      await this._db.rpush(listKey, pktKey)
    }
  }

  async outgoingUpdate (client, packet) {
    if (!('brokerId' in packet && 'messageId' in packet)) {
      augmentWithBrokerData(this, client, packet)
    }
    await updateWithClientData(this, client, packet)
  }

  async outgoingClearMessageId (client, packet) {
    const clientListKey = outgoingKey(client.id)
    const messageIdKey = outgoingIdKey(client.id, packet.messageId)

    const clientKey = this.messageIdCache.get(messageIdKey)
    this.messageIdCache.remove(messageIdKey)

    if (!clientKey) {
      return
    }

    // TODO can be cached in case of wildcard deliveries
    const buf = await this._db.getBuffer(clientKey)

    let origPacket
    let pktKey
    let countKey

    if (buf) {
      origPacket = msgpack.decode(buf)
      origPacket.messageId = packet.messageId

      pktKey = packetKey(origPacket.brokerId, origPacket.brokerCounter)
      countKey = packetCountKey(origPacket.brokerId, origPacket.brokerCounter)

      if (clientKey !== pktKey) { // qos=2
        await this._db.del(clientKey)
      }
    }
    await this._db.lrem(clientListKey, 0, pktKey)

    const remained = await this._db.decr(countKey)
    if (remained === 0) {
      if (this.hasClusters) {
        // do not remove multiple keys at once, fails in clusters
        await this._db.del(pktKey)
        await this._db.del(countKey)
      } else {
        await this._db.del(pktKey, countKey)
      }
    }
    return origPacket
  }

  outgoingStream (client) {
    const clientListKey = outgoingKey(client.id)
    const db = this._db
    const maxSessionDelivery = this.maxSessionDelivery

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
    await this._db.set(key, msgpack.encode(newp))
  }

  async incomingGetPacket (client, packet) {
    const key = incomingKey(client.id, packet.messageId)
    const buf = await this._db.getBuffer(key)
    if (!buf) {
      throw new Error('no such packet')
    }
    return msgpack.decode(buf)
  }

  async incomingDelPacket (client, packet) {
    const key = incomingKey(client.id, packet.messageId)
    await this._db.del(key)
  }

  async putWill (client, packet) {
    const key = willKey(this.broker.id, client.id)
    packet.clientId = client.id
    packet.brokerId = this.broker.id
    this._db.lrem(WILLSKEY, 0, key) // Remove duplicates
    this._db.rpush(WILLSKEY, key)
    await this._db.setBuffer(key, msgpack.encode(packet))
  }

  async getWill (client) {
    const key = willKey(this.broker.id, client.id)
    const packet = await this._db.getBuffer(key)
    const result = packet ? msgpack.decode(packet) : null
    return result
  }

  async delWill (client) {
    const key = willKey(client.brokerId, client.id)
    this._db.lrem(WILLSKEY, 0, key)
    const packet = await this._db.getBuffer(key)
    const result = packet ? msgpack.decode(packet) : null
    await this._db.del(key)
    return result
  }

  streamWill (brokers) {
    return createWillStream(this._db, brokers, this.maxWills)
  }

  getClientList (topic) {
    const entries = this._trie.match(topic, topic)
    return getClientIdFromEntries(entries)
  }

  #buildAugment (listKey) {
    const that = this
    return function decodeAndAugment (key, enc, cb) {
      that._db.getBuffer(key, function decodeMessage (err, result) {
        let decoded
        if (result) {
          decoded = msgpack.decode(result)
        }
        if (err || !decoded) {
          that._db.lrem(listKey, 0, key)
        }
        cb(err, decoded)
      })
    }
  }

  async destroy (cb) {
    await this._db.disconnect()
  }
}

module.exports = {
  AsyncRedisPersistence
}
