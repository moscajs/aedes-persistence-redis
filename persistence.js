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
var Qlobber = require('qlobber').Qlobber
var qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some: '#'
}
var clientKey = 'client:'
var clientsKey = 'clients'
var subsKey = 'subs'
var willKey = 'will'
var retainedKey = 'retained'
var outgoingKey = 'outgoing:'

function RedisPersistence (opts) {
  if (!(this instanceof RedisPersistence)) {
    return new RedisPersistence(opts)
  }

  opts = opts || {}
  this.maxSessionDelivery = opts.maxSessionDelivery || 1000
  this._db = new Redis(opts)

  this.messageIdCache = {}

  var that = this
  this._decodeAndAugment = function decodeAndAugment (chunk, enc, cb) {
    that._db.getBuffer(chunk, function decodeMessage (err, result) {
      var decoded
      if (result) {
        decoded = msgpack.decode(result)
      }
      cb(err, decoded)
    })
  }

  this._getChunk = function (chunk, enc, cb) {
    that._db.hgetBuffer(retainedKey, chunk, cb)
  }

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

RedisPersistence.prototype.createRetainedStreamCombi = function (patterns) {
  var that = this
  var qlobber = new Qlobber(qlobberOpts)

  for (var i = 0; i < patterns.length; i++) {
    qlobber.add(patterns[i], true)
  }

  var stream = through.obj(that._getChunk)

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
    if (qlobber.match(keys[i]).length > 0) {
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
  var offlines = []
  var published = 0
  var errored

  for (var i = 0; i < subs.length; i++) {
    var sub = subs[i]
    toStore[sub.topic] = sub.qos
    if (sub.qos > 0) {
      offlines.push(sub.topic)
    }
  }

  this._db.sadd(subsKey, offlines, finish)
  this._db.sadd(clientsKey, client.id, finish)
  this._db.hmset(clientSubKey, toStore, finish)

  this._addedSubscriptions(client, subs, finish)

  function finish (err) {
    errored = err
    published++
    if (published === 4) {
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

  var removableTopics = []
  var errored = false

  for (var i = 0; i < subs.length; i++) {
    if (this._matcher.match(subs[i]).length === 1) {
      removableTopics.push(subs[i])
    }
  }

  var that = this
  this._db.hdel(clientSubKey, subs, function subKeysRemoved (err) {
    if (err) {
      errored = true
      return cb(err)
    }

    if (removableTopics.length > 0) {
      that._db.srem(subsKey, removableTopics)
    }

    that._db.exists(clientSubKey, function checkAllSubsRemoved (err, subCount) {
      if (err) {
        errored = true
        return cb(err)
      }
      if (subCount === 0) {
        that._db.srem(clientsKey, client.id)
      }
    })

    that._removedSubscriptions(client, subs.map(toSub), function () {
      if (!errored) {
        cb(null, client)
      }
    })
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
  var clientsCount = -1
  var subsCount = -1

  this._db.scard(clientsKey, function countOfflineClients (err, count) {
    if (err) {
      return cb(err)
    }

    clientsCount = parseInt(count) || 0

    if (subsCount >= 0) {
      cb(null, subsCount, clientsCount)
    }
  })

  this._db.scard(subsKey, function countSubscriptions (err, count) {
    if (err) {
      return cb(err)
    }

    subsCount = parseInt(count) || 0

    if (clientsCount >= 0) {
      cb(null, subsCount, clientsCount)
    }
  })
}

RedisPersistence.prototype.subscriptionsByTopic = function (topic, cb) {
  if (!this.ready) {
    this.once('ready', this.subscriptionsByTopic.bind(this, topic, cb))
    return this
  }

  var result = this._matcher.match(topic)

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
      cb(err, {clientHash: hash, clientId: clientId})
    })
  }, function emitReady (cb) {
    that.ready = true
    that.emit('ready')
    cb()
  })
  .on('data', function processKeys (data) {
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
    that._matcher.add(topic, {
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
  if (!Array.isArray(subs)) {
    subs = [subs]
  }
  var count = 0
  var errored = false
  var packetKey = 'packet:' + packet.brokerId + ':' + packet.brokerCounter
  var countKey = 'packet:' + packet.brokerId + ':' + packet.brokerCounter + ':offlineCount'
  this._db.mset(
    packetKey, msgpack.encode(new Packet(packet)),
    countKey, subs.length,
    finish
  )
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
    if (count === subs.length + 1 && !errored) {
      cb(null, packet)
    }
  }
}

function updateWithClientData (that, client, packet, cb) {
  var messageIdKey = 'outgoing-id:' + client.id + ':' + packet.messageId
  var clientListKey = outgoingKey + client.id
  var packetKey = 'packet:' + packet.brokerId + ':' + packet.brokerCounter

  if (packet.cmd && packet.cmd !== 'pubrel') {
    that.messageIdCache[messageIdKey] = packetKey
    return cb(null, client, packet)
  }

  var clientUpdateKey = outgoingKey + client.id + ':' + packet.brokerId + ':' + packet.brokerCounter
  that.messageIdCache[messageIdKey] = clientUpdateKey

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

  that._db.set(clientUpdateKey, msgpack.encode(packet), function setPostKey (err, result) {
    if (err) {
      return cb(err, client, packet)
    }

    if (result !== 'OK') {
      cb(new Error('no such packet'), client, packet)
    } else {
      finish()
    }
  })

  function finish (err) {
    if (++count === 2) {
      cb(err, client, packet)
    }
  }
}

function augmentWithBrokerData (that, client, packet, cb) {
  var messageIdKey = 'outgoing-id:' + client.id + ':' + packet.messageId

  var key = that.messageIdCache[messageIdKey]
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
  var messageIdKey = 'outgoing-id:' + client.id + ':' + packet.messageId

  var clientKey = this.messageIdCache[messageIdKey]
  this.messageIdCache[messageIdKey] = null

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
    var origPacket = msgpack.decode(buf)
    origPacket.messageId = packet.messageId

    var packetKey = 'packet:' + origPacket.brokerId + ':' + origPacket.brokerCounter
    var countKey = 'packet:' + origPacket.brokerId + ':' + origPacket.brokerCounter + ':offlineCount'

    if (clientKey !== packetKey) { // qos=2
      that._db.del(clientKey, finish)
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
        that._db.del(packetKey, finish)
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
  var stream = throughv.obj(this._decodeAndAugment)

  this._db.lrange(outgoingKey + client.id, 0, this.maxSessionDelivery, function lrangeResult (err, results) {
    if (err) {
      stream.emit('error', err)
    } else {
      for (var i = 0, l = results.length; i < l; i++) {
        stream.write(results[i])
      }
      stream.end()
    }
  })

  return stream
}

RedisPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  var key = 'incoming:' + client.id + ':' + packet.messageId
  var newp = new Packet(packet)
  newp.messageId = packet.messageId
  this._db.set(key, msgpack.encode(newp), cb)
}

RedisPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  var key = 'incoming:' + client.id + ':' + packet.messageId
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
  var key = 'incoming:' + client.id + ':' + packet.messageId
  this._db.del(key, cb)
}

RedisPersistence.prototype.putWill = function (client, packet, cb) {
  var key = 'will:' + this.broker.id + ':' + client.id
  packet.clientId = client.id
  packet.brokerId = this.broker.id
  this._db.rpush(willKey, key)
  this._db.setBuffer(key, msgpack.encode(packet), encodeBuffer)

  function encodeBuffer (err) {
    cb(err, client)
  }
}

RedisPersistence.prototype.getWill = function (client, cb) {
  var key = 'will:' + this.broker.id + ':' + client.id
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
  var key = 'will:' + client.brokerId + ':' + client.id
  var result = null
  var that = this
  this._db.lrem(willKey, 0, key)
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
  var stream = throughv.obj(this._decodeAndAugment)

  this._db.lrange(willKey, 0, 10000, streamWill)

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
  var clientIds = this._matcher.match(topic).map(toClientIds)

  function pushClientList (size, next) {
    if (clientIds.length === 0) {
      return next(null, null)
    }
    var chunk = clientIds.slice(0, 1)
    clientIds = clientIds.slice(1)
    next(null, chunk[0])
  }

  function toClientIds (matched) {
    return matched.clientId
  }

  return from.obj(pushClientList)
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
