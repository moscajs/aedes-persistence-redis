const Redis = require('ioredis')
const from = require('from2')
const through = require('through2')
const throughv = require('throughv')
const msgpack = require('msgpack-lite')
const pump = require('pump')
const CachedPersistence = require('aedes-cached-persistence')
const Packet = CachedPersistence.Packet
const HLRU = require('hashlru')
const QlobberTrue = require('qlobber').QlobberTrue
const qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#',
  match_empty_levels: true
}
const clientKey = 'client:'
const clientsKey = 'clients'
const willsKey = 'will'
const willKey = 'will:'
const retainedKey = 'retained'
const outgoingKey = 'outgoing:'
const outgoingIdKey = 'outgoing-id:'
const incomingKey = 'incoming:'

class RedisPersistence extends CachedPersistence {
  constructor (opts = {}) {
    super()
    this.maxSessionDelivery = opts.maxSessionDelivery || 1000
    this.packetTTL = opts.packetTTL || (() => { return 0 })

    this.messageIdCache = HLRU(100000)

    if (opts.cluster) {
      this._db = new Redis.Cluster(opts.cluster)
    } else {
      this._db = opts.conn || new Redis(opts)
    }

    this._getRetainedChunkBound = this._getRetainedChunk.bind(this)
    CachedPersistence.call(this, opts)
  }

  storeRetained (packet, cb) {
    if (packet.payload.length === 0) {
      this._db.hdel(retainedKey, packet.topic, cb)
    } else {
      this._db.hset(retainedKey, packet.topic, msgpack.encode(packet), cb)
    }
  }

  _getRetainedChunk (chunk, enc, cb) {
    this._db.hgetBuffer(retainedKey, chunk, cb)
  }

  createRetainedStreamCombi (patterns) {
    const that = this
    const qlobber = new QlobberTrue(qlobberOpts)

    for (const pattern of patterns) {
      qlobber.add(pattern)
    }

    const stream = through.obj(that._getRetainedChunkBound)

    this._db.hkeys(retainedKey, function getKeys (err, keys) {
      if (err) {
        stream.emit('error', err)
      } else {
        matchRetained(stream, keys, qlobber)
      }
    })

    return pump(stream, throughv.obj(decodeRetainedPacket))
  }

  createRetainedStream (pattern) {
    return this.createRetainedStreamCombi([pattern])
  }

  addSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
      return
    }

    const clientSubKey = clientKey + client.id

    const toStore = {}
    let published = 0
    let errored

    for (const sub of subs) {
      toStore[sub.topic] = sub.qos
    }

    this._db.sadd(clientsKey, client.id, finish)
    this._db.hmset(clientSubKey, toStore, finish)

    this._addedSubscriptions(client, subs, finish)

    function finish (err) {
      errored = err
      published++
      if (published === 3) {
        cb(errored, client)
      }
    }
  }

  removeSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
      return
    }

    const clientSubKey = clientKey + client.id

    let errored = false
    let outstanding = 0

    function check (err) {
      if (err) {
        if (!errored) {
          errored = true
          cb(err)
        }
      }

      if (errored) {
        return
      }

      outstanding--
      if (outstanding === 0) {
        cb(null, client)
      }
    }

    const that = this
    this._db.hdel(clientSubKey, subs, function subKeysRemoved (err) {
      if (err) {
        return cb(err)
      }

      outstanding++
      that._db.exists(clientSubKey, function checkAllSubsRemoved (err, subCount) {
        if (err) {
          return check(err)
        }
        if (subCount === 0) {
          outstanding++
          that._db.del(outgoingKey + client.id, check)
          return that._db.srem(clientsKey, client.id, check)
        }
        check()
      })

      outstanding++
      that._removedSubscriptions(client, subs.map(toSub), check)
    })
  }

  subscriptionsByClient (client, cb) {
    const clientSubKey = clientKey + client.id

    this._db.hgetall(clientSubKey, function returnSubs (err, subs) {
      const toReturn = returnSubsForClient(subs)
      cb(err, toReturn.length > 0 ? toReturn : null, client)
    })
  }

  countOffline (cb) {
    const that = this

    this._db.scard(clientsKey, function countOfflineClients (err, count) {
      if (err) {
        return cb(err)
      }

      cb(null, that._trie.subscriptionsCount, parseInt(count) || 0)
    })
  }

  subscriptionsByTopic (topic, cb) {
    if (!this.ready) {
      this.once('ready', this.subscriptionsByTopic.bind(this, topic, cb))
      return this
    }

    const result = this._trie.match(topic)

    cb(null, result)
  }

  _setup () {
    if (this.ready) {
      return
    }

    const that = this

    const hgetallStream = throughv.obj(function getStream (clientId, enc, cb) {
      const clientSubKey = clientKey + clientId
      that._db.hgetall(clientSubKey, function clientHash (err, hash) {
        cb(err, { clientHash: hash, clientId })
      })
    }, function emitReady (cb) {
      that.ready = true
      that.emit('ready')
      cb()
    }).on('data', function processKeys (data) {
      processKeysForClient(data.clientId, data.clientHash, that)
    })

    this._db.smembers(clientsKey, function smembers (err, clientIds) {
      if (err) {
        hgetallStream.emit('error', err)
      } else {
        for (const clientId of clientIds) {
          hgetallStream.write(clientId)
        }
        hgetallStream.end()
      }
    })
  }

  outgoingEnqueue (sub, packet, cb) {
    this.outgoingEnqueueCombi([sub], packet, cb)
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    if (!subs || subs.length === 0) {
      return cb(null, packet)
    }
    let count = 0
    let outstanding = 1
    let errored = false
    const packetKey = `packet:${packet.brokerId}:${packet.brokerCounter}`
    const countKey = `packet:${packet.brokerId}:${packet.brokerCounter}:offlineCount`
    const ttl = this.packetTTL(packet)

    const encoded = msgpack.encode(new Packet(packet))

    this._db.mset(packetKey, encoded, countKey, subs.length, finish)
    if (ttl > 0) {
      outstanding += 2
      this._db.expire(packetKey, ttl, finish)
      this._db.expire(countKey, ttl, finish)
    }

    for (const sub of subs) {
      const listKey = outgoingKey + sub.clientId
      this._db.rpush(listKey, packetKey, finish)
    }

    function finish (err) {
      count++
      if (err) {
        errored = err
        return cb(err)
      }
      if (count === (subs.length + outstanding) && !errored) {
        cb(null, packet)
      }
    }
  }

  outgoingUpdate (client, packet, cb) {
    const that = this
    if (packet.brokerId && packet.messageId) {
      updateWithClientData(this, client, packet, cb)
    } else {
      augmentWithBrokerData(this, client, packet, function updateClient (err) {
        if (err) { return cb(err, client, packet) }

        updateWithClientData(that, client, packet, cb)
      })
    }
  }

  outgoingClearMessageId (client, packet, cb) {
    const that = this
    const clientListKey = outgoingKey + client.id
    const messageIdKey = `${outgoingIdKey + client.id}:${packet.messageId}`

    const clientKey = this.messageIdCache.get(messageIdKey)
    this.messageIdCache.remove(messageIdKey)

    if (!clientKey) {
      return cb(null, packet)
    }

    let count = 0
    let errored = false

    // TODO can be cached in case of wildcard deliveries
    this._db.getBuffer(clientKey, function clearMessageId (err, buf) {
      let origPacket
      let packetKey
      let countKey
      if (err) {
        errored = err
        return cb(err)
      }
      if (buf) {
        origPacket = msgpack.decode(buf)
        origPacket.messageId = packet.messageId

        packetKey = `packet:${origPacket.brokerId}:${origPacket.brokerCounter}`
        countKey = `packet:${origPacket.brokerId}:${origPacket.brokerCounter}:offlineCount`

        if (clientKey !== packetKey) { // qos=2
          that._db.del(clientKey, finish)
        } else {
          finish()
        }
      } else {
        finish()
      }

      that._db.lrem(clientListKey, 0, packetKey, finish)

      that._db.decr(countKey, (err, remained) => {
        if (err) {
          errored = err
          return cb(err)
        }
        if (remained === 0) {
          that._db.del(packetKey, countKey, finish)
        } else {
          finish()
        }
      })

      function finish (err) {
        count++
        if (err) {
          errored = err
          return cb(err)
        }
        if (count === 3 && !errored) {
          cb(err, origPacket)
        }
      }
    })
  }

  outgoingStream (client) {
    const clientListKey = outgoingKey + client.id
    const stream = throughv.obj(this._buildAugment(clientListKey))

    this._db.lrange(clientListKey, 0, this.maxSessionDelivery, lrangeResult)

    function lrangeResult (err, results) {
      if (err) {
        stream.emit('error', err)
      } else {
        for (const result of results) {
          stream.write(result)
        }
        stream.end()
      }
    }

    return stream
  }

  incomingStorePacket (client, packet, cb) {
    const key = `${incomingKey + client.id}:${packet.messageId}`
    const newp = new Packet(packet)
    newp.messageId = packet.messageId
    this._db.set(key, msgpack.encode(newp), cb)
  }

  incomingGetPacket (client, packet, cb) {
    const key = `${incomingKey + client.id}:${packet.messageId}`
    this._db.getBuffer(key, function decodeBuffer (err, buf) {
      if (err) {
        return cb(err)
      }

      if (!buf) {
        return cb(new Error('no such packet'))
      }

      cb(null, msgpack.decode(buf), client)
    })
  }

  incomingDelPacket (client, packet, cb) {
    const key = `${incomingKey + client.id}:${packet.messageId}`
    this._db.del(key, cb)
  }

  putWill (client, packet, cb) {
    const key = `${willKey + this.broker.id}:${client.id}`
    packet.clientId = client.id
    packet.brokerId = this.broker.id
    this._db.rpush(willsKey, key)
    this._db.setBuffer(key, msgpack.encode(packet), encodeBuffer)

    function encodeBuffer (err) {
      cb(err, client)
    }
  }

  getWill (client, cb) {
    const key = `${willKey + this.broker.id}:${client.id}`
    this._db.getBuffer(key, function getWillForClient (err, packet) {
      if (err) { return cb(err) }

      let result = null

      if (packet) {
        result = msgpack.decode(packet)
      }

      cb(null, result, client)
    })
  }

  delWill (client, cb) {
    const key = `${willKey + client.brokerId}:${client.id}`
    let result = null
    const that = this
    this._db.lrem(willsKey, 0, key)
    this._db.getBuffer(key, function getClientWill (err, packet) {
      if (err) { return cb(err) }

      if (packet) {
        result = msgpack.decode(packet)
      }

      that._db.del(key, function deleteWill (err) {
        cb(err, result, client)
      })
    })
  }

  streamWill (brokers) {
    const stream = throughv.obj(this._buildAugment(willsKey))

    this._db.lrange(willsKey, 0, 10000, streamWill)

    function streamWill (err, results) {
      if (err) {
        stream.emit('error', err)
      } else {
        for (const result of results) {
          if (!brokers || !brokers[result.split(':')[1]]) {
            stream.write(result)
          }
        }
        stream.end()
      }
    }
    return stream
  }

  getClientList (topic) {
    let entries = this._trie.match(topic, topic)

    function pushClientList (size, next) {
      if (entries.length === 0) {
        return next(null, null)
      }
      const chunk = entries.slice(0, 1)
      entries = entries.slice(1)
      next(null, chunk[0].clientId)
    }

    return from.obj(pushClientList)
  }

  _buildAugment (listKey) {
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

  destroy (cb) {
    const that = this
    CachedPersistence.prototype.destroy.call(this, function disconnect () {
      that._db.disconnect()

      if (cb) {
        that._db.on('end', cb)
      }
    })
  }
}

function matchRetained (stream, keys, qlobber) {
  for (const key of keys) {
    if (qlobber.test(key)) {
      stream.write(key)
    }
  }
  stream.end()
}

function decodeRetainedPacket (chunk, enc, cb) {
  cb(null, msgpack.decode(chunk))
}

function toSub (topic) {
  return {
    topic
  }
}

function returnSubsForClient (subs) {
  const subKeys = Object.keys(subs)

  const toReturn = []

  if (subKeys.length === 0) {
    return toReturn
  }

  for (const subKey of subKeys) {
    toReturn.push({
      topic: subKey,
      qos: parseInt(subs[subKey])
    })
  }

  return toReturn
}

function processKeysForClient (clientId, clientHash, that) {
  const topics = Object.keys(clientHash)

  for (const topic of topics) {
    that._trie.add(topic, {
      clientId,
      topic,
      qos: clientHash[topic]
    })
  }
}

function updateWithClientData (that, client, packet, cb) {
  const messageIdKey = `${outgoingIdKey + client.id}:${packet.messageId}`
  const clientListKey = outgoingKey + client.id
  const packetKey = `packet:${packet.brokerId}:${packet.brokerCounter}`

  const ttl = that.packetTTL(packet)
  if (packet.cmd && packet.cmd !== 'pubrel') { // qos=1
    that.messageIdCache.set(messageIdKey, packetKey)
    if (ttl > 0) {
      return that._db.set(packetKey, msgpack.encode(packet), 'EX', ttl, updatePacket)
    } else {
      return that._db.set(packetKey, msgpack.encode(packet), updatePacket)
    }
  }

  // qos=2
  const clientUpdateKey = `${outgoingKey + client.id}:${packet.brokerId}:${packet.brokerCounter}`
  that.messageIdCache.set(messageIdKey, clientUpdateKey)

  let count = 0
  that._db.lrem(clientListKey, 0, packetKey, (err, removed) => {
    if (err) {
      return cb(err)
    }
    if (removed === 1) {
      that._db.rpush(clientListKey, clientUpdateKey, finish)
    } else {
      finish()
    }
  })

  const encoded = msgpack.encode(packet)

  if (ttl > 0) {
    that._db.set(clientUpdateKey, encoded, 'EX', ttl, setPostKey)
  } else {
    that._db.set(clientUpdateKey, encoded, setPostKey)
  }

  function updatePacket (err, result) {
    if (err) {
      return cb(err, client, packet)
    }

    if (result !== 'OK') {
      cb(new Error('no such packet'), client, packet)
    } else {
      cb(null, client, packet)
    }
  }

  function setPostKey (err, result) {
    if (err) {
      return cb(err, client, packet)
    }

    if (result !== 'OK') {
      cb(new Error('no such packet'), client, packet)
    } else {
      finish()
    }
  }

  function finish (err) {
    if (++count === 2) {
      cb(err, client, packet)
    }
  }
}

function augmentWithBrokerData (that, client, packet, cb) {
  const messageIdKey = `${outgoingIdKey + client.id}:${packet.messageId}`

  const key = that.messageIdCache.get(messageIdKey)
  if (!key) {
    return cb(new Error('unknown key'))
  }
  const tokens = key.split(':')
  packet.brokerId = tokens[tokens.length - 2]
  packet.brokerCounter = tokens[tokens.length - 1]

  cb(null)
}

module.exports = (opts) => new RedisPersistence(opts)
