'use strict'

var Redis = require('ioredis')
var from = require('from2')
var through = require('through2')
var throughv = require('throughv')
var msgpack = require('msgpack-lite')
var pump = require('pump')
var CachedPersistence = require('aedes-cached-persistence')
var Packet = CachedPersistence.Packet
var inherits = require('util').inherits
var HLRU = require('hashlru')
var QlobberTrue = require('qlobber').QlobberTrue
var qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#'
}
var clientKey = 'client:'
var clientsKey = 'clients'
var willsKey = 'will'
var willKey = 'will:'
var retainedKey = 'retained'
var outgoingKey = 'outgoing:'
var outgoingIdKey = 'outgoing-id:'
var incomingKey = 'incoming:'

function RedisPersistence (opts) {
  if (!(this instanceof RedisPersistence)) {
    return new RedisPersistence(opts)
  }

  opts = opts || {}
  this.maxSessionDelivery = opts.maxSessionDelivery || 1000
  this.packetTTL = opts.packetTTL || function () { return 0 }

  this.messageIdCache = HLRU(100000)

  this._db = opts.conn || new Redis(opts)

  this._getRetainedChunkBound = this._getRetainedChunk.bind(this)
  CachedPersistence.call(this, opts)
}

inherits(RedisPersistence, CachedPersistence)

RedisPersistence.prototype.storeRetained = function (packet, cb) {
  if (packet.payload.length === 0) {
    this._db.hdel(retainedKey, packet.topic, cb)
  } else {
    this._db.hset(retainedKey, packet.topic, msgpack.encode(packet), cb)
  }
}

RedisPersistence.prototype._getRetainedChunk = function (chunk, enc, cb) {
  this._db.hgetBuffer(retainedKey, chunk, cb)
}

RedisPersistence.prototype.createRetainedStreamCombi = function (patterns) {
  var that = this
  var qlobber = new QlobberTrue(qlobberOpts)

  for (var i = 0; i < patterns.length; i++) {
    qlobber.add(patterns[i])
  }

  var stream = through.obj(that._getRetainedChunkBound)

  this._db.hkeys(retainedKey, function getKeys (err, keys) {
    if (err) {
      stream.emit('error', err)
    } else {
      matchRetained(stream, keys, qlobber)
    }
  })

  return pump(stream, throughv.obj(decodeRetainedPacket))
}

RedisPersistence.prototype.createRetainedStream = function (pattern) {
  return this.createRetainedStreamCombi([pattern])
}

function matchRetained (stream, keys, qlobber) {
  for (var i = 0, l = keys.length; i < l; i++) {
    if (qlobber.test(keys[i])) {
      stream.write(keys[i])
    }
  }
  stream.end()
}

function decodeRetainedPacket (chunk, enc, cb) {
  cb(null, msgpack.decode(chunk))
}

RedisPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
    return
  }

  var clientSubKey = clientKey + client.id

  var toStore = {}
  var published = 0
  var errored

  for (var i = 0; i < subs.length; i++) {
    var sub = subs[i]
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

RedisPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
    return
  }

  var clientSubKey = clientKey + client.id

  var errored = false
  var outstanding = 0

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

  var that = this
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

function toSub (topic) {
  return {
    topic: topic
  }
}

RedisPersistence.prototype.subscriptionsByClient = function (client, cb) {
  var clientSubKey = clientKey + client.id

  this._db.hgetall(clientSubKey, function returnSubs (err, subs) {
    var toReturn = returnSubsForClient(subs)
    cb(err, toReturn.length > 0 ? toReturn : null, client)
  })
}

function returnSubsForClient (subs) {
  var subKeys = Object.keys(subs)

  var toReturn = []

  if (subKeys.length === 0) {
    return toReturn
  }

  for (var i = 0; i < subKeys.length; i++) {
    toReturn.push({
      topic: subKeys[i],
      qos: parseInt(subs[subKeys[i]])
    })
  }

  return toReturn
}

RedisPersistence.prototype.countOffline = function (cb) {
  var that = this

  this._db.scard(clientsKey, function countOfflineClients (err, count) {
    if (err) {
      return cb(err)
    }

    cb(null, that._trie.subscriptionsCount, parseInt(count) || 0)
  })
}

RedisPersistence.prototype.subscriptionsByTopic = function (topic, cb) {
  if (!this.ready) {
    this.once('ready', this.subscriptionsByTopic.bind(this, topic, cb))
    return this
  }

  var result = this._trie.match(topic)

  cb(null, result)
}

RedisPersistence.prototype._setup = function () {
  if (this.ready) {
    return
  }

  var that = this

  var hgetallStream = throughv.obj(function getStream (clientId, enc, cb) {
    var clientSubKey = clientKey + clientId
    that._db.hgetall(clientSubKey, function clientHash (err, hash) {
      cb(err, { clientHash: hash, clientId: clientId })
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
      for (var i = 0, l = clientIds.length; i < l; i++) {
        hgetallStream.write(clientIds[i])
      }
      hgetallStream.end()
    }
  })
}

function processKeysForClient (clientId, clientHash, that) {
  var topics = Object.keys(clientHash)
  for (var i = 0; i < topics.length; i++) {
    var topic = topics[i]
    that._trie.add(topic, {
      clientId: clientId,
      topic: topic,
      qos: clientHash[topic]
    })
  }
}

RedisPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  this.outgoingEnqueueCombi([sub], packet, cb)
}

RedisPersistence.prototype.outgoingEnqueueCombi = function (subs, packet, cb) {
  if (!subs || subs.length === 0) {
    return cb(null, packet)
  }
  var count = 0
  var outstanding = 1
  var errored = false
  var packetKey = 'packet:' + packet.brokerId + ':' + packet.brokerCounter
  var countKey = 'packet:' + packet.brokerId + ':' + packet.brokerCounter + ':offlineCount'
  var ttl = this.packetTTL(packet)
  var encoded = msgpack.encode(new Packet(packet))

  this._db.mset(packetKey, encoded, countKey, subs.length, finish)
  if (ttl > 0) {
    outstanding += 2
    this._db.expire(packetKey, ttl, finish)
    this._db.expire(countKey, ttl, finish)
  }

  for (var i = 0; i < subs.length; i++) {
    var listKey = outgoingKey + subs[i].clientId
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

function updateWithClientData (that, client, packet, cb) {
  var messageIdKey = outgoingIdKey + client.id + ':' + packet.messageId
  var clientListKey = outgoingKey + client.id
  var packetKey = 'packet:' + packet.brokerId + ':' + packet.brokerCounter

  if (packet.cmd && packet.cmd !== 'pubrel') { // qos=1
    that.messageIdCache.set(messageIdKey, packetKey)
    return cb(null, client, packet)
  }

  // qos=2
  var clientUpdateKey = outgoingKey + client.id + ':' + packet.brokerId + ':' + packet.brokerCounter
  that.messageIdCache.set(messageIdKey, clientUpdateKey)

  var count = 0
  that._db.lrem(clientListKey, 0, packetKey, function (err, removed) {
    if (err) {
      return cb(err)
    }
    if (removed === 1) {
      that._db.rpush(clientListKey, clientUpdateKey, finish)
    } else {
      finish()
    }
  })

  var ttl = that.packetTTL(packet)
  var encoded = msgpack.encode(packet)

  if (ttl > 0) {
    that._db.set(clientUpdateKey, encoded, 'EX', ttl, setPostKey)
  } else {
    that._db.set(clientUpdateKey, encoded, setPostKey)
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
  var messageIdKey = outgoingIdKey + client.id + ':' + packet.messageId

  var key = that.messageIdCache.get(messageIdKey)
  if (!key) {
    return cb(new Error('unknown key'))
  }
  var tokens = key.split(':')
  packet.brokerId = tokens[tokens.length - 2]
  packet.brokerCounter = tokens[tokens.length - 1]

  cb(null)
}

RedisPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
  var that = this
  if (packet.brokerId) {
    updateWithClientData(this, client, packet, cb)
  } else {
    augmentWithBrokerData(this, client, packet, function updateClient (err) {
      if (err) { return cb(err, client, packet) }

      updateWithClientData(that, client, packet, cb)
    })
  }
}

RedisPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
  var that = this
  var clientListKey = outgoingKey + client.id
  var messageIdKey = outgoingIdKey + client.id + ':' + packet.messageId

  var clientKey = this.messageIdCache.get(messageIdKey)
  this.messageIdCache.remove(messageIdKey)

  if (!clientKey) {
    return cb(null, packet)
  }

  var count = 0
  var errored = false

  // TODO can be cached in case of wildcard deliveries
  this._db.getBuffer(clientKey, function clearMessageId (err, buf) {
    if (err) {
      errored = err
      return cb(err)
    }
    if (buf) {
      var origPacket = msgpack.decode(buf)
      origPacket.messageId = packet.messageId

      var packetKey = 'packet:' + origPacket.brokerId + ':' + origPacket.brokerCounter
      var countKey = 'packet:' + origPacket.brokerId + ':' + origPacket.brokerCounter + ':offlineCount'

      if (clientKey !== packetKey) { // qos=2
        that._db.del(clientKey, finish)
      } else {
        finish()
      }
    } else {
      finish()
    }

    that._db.lrem(clientListKey, 0, packetKey, finish)

    that._db.decr(countKey, function (err, remained) {
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

RedisPersistence.prototype.outgoingStream = function (client) {
  var clientListKey = outgoingKey + client.id
  var stream = throughv.obj(this._buildAugment(clientListKey))

  this._db.lrange(clientListKey, 0, this.maxSessionDelivery, lrangeResult)

  function lrangeResult (err, results) {
    if (err) {
      stream.emit('error', err)
    } else {
      for (var i = 0, l = results.length; i < l; i++) {
        stream.write(results[i])
      }
      stream.end()
    }
  }

  return stream
}

RedisPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  var key = incomingKey + client.id + ':' + packet.messageId
  var newp = new Packet(packet)
  newp.messageId = packet.messageId
  this._db.set(key, msgpack.encode(newp), cb)
}

RedisPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  var key = incomingKey + client.id + ':' + packet.messageId
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

RedisPersistence.prototype.incomingDelPacket = function (client, packet, cb) {
  var key = incomingKey + client.id + ':' + packet.messageId
  this._db.del(key, cb)
}

RedisPersistence.prototype.putWill = function (client, packet, cb) {
  var key = willKey + this.broker.id + ':' + client.id
  packet.clientId = client.id
  packet.brokerId = this.broker.id
  this._db.rpush(willsKey, key)
  this._db.setBuffer(key, msgpack.encode(packet), encodeBuffer)

  function encodeBuffer (err) {
    cb(err, client)
  }
}

RedisPersistence.prototype.getWill = function (client, cb) {
  var key = willKey + this.broker.id + ':' + client.id
  this._db.getBuffer(key, function getWillForClient (err, packet) {
    if (err) { return cb(err) }

    var result = null

    if (packet) {
      result = msgpack.decode(packet)
    }

    cb(null, result, client)
  })
}

RedisPersistence.prototype.delWill = function (client, cb) {
  var key = willKey + client.brokerId + ':' + client.id
  var result = null
  var that = this
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

RedisPersistence.prototype.streamWill = function (brokers) {
  var stream = throughv.obj(this._buildAugment(willsKey))

  this._db.lrange(willsKey, 0, 10000, streamWill)

  function streamWill (err, results) {
    if (err) {
      stream.emit('error', err)
    } else {
      for (var i = 0, l = results.length; i < l; i++) {
        if (!brokers || !brokers[results[i].split(':')[1]]) {
          stream.write(results[i])
        }
      }
      stream.end()
    }
  }
  return stream
}

RedisPersistence.prototype.getClientList = function (topic) {
  var entries = this._trie.match(topic, topic)

  function pushClientList (size, next) {
    if (entries.length === 0) {
      return next(null, null)
    }
    var chunk = entries.slice(0, 1)
    entries = entries.slice(1)
    next(null, chunk[0].clientId)
  }

  return from.obj(pushClientList)
}

RedisPersistence.prototype._buildAugment = function (listKey) {
  var that = this
  return function decodeAndAugment (key, enc, cb) {
    that._db.getBuffer(key, function decodeMessage (err, result) {
      var decoded
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

RedisPersistence.prototype.destroy = function (cb) {
  var that = this
  CachedPersistence.prototype.destroy.call(this, function disconnect () {
    that._db.disconnect()

    if (cb) {
      that._db.on('end', cb)
    }
  })
}

module.exports = RedisPersistence
