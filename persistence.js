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
var nextTick = require('process-nextick-args')
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
  this._pipeline = null

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

RedisPersistence.prototype._getPipeline = function () {
  if (!this._pipeline) {
    this._pipeline = this._db.pipeline()
    nextTick(execPipeline, this)
  }
  return this._pipeline
}

function execPipeline (that) {
  that._pipeline.exec()
  that._pipeline = null
}

RedisPersistence.prototype.storeRetained = function (packet, cb) {
  if (packet.payload.length === 0) {
    this._getPipeline().hdel(retainedKey, packet.topic, cb)
  } else {
    this._getPipeline().hset(retainedKey, packet.topic, msgpack.encode(packet), cb)
  }
}

RedisPersistence.prototype.createRetainedStream = function (pattern) {
  var that = this
  var qlobber = new Qlobber(qlobberOpts)
  qlobber.add(pattern, true)

  var stream = through.obj(that._getChunk)

  this._getPipeline().hkeys(retainedKey, function getKeys (err, keys) {
    if (err) {
      stream.emit('error', err)
    } else {
      matchRetained(stream, keys, qlobber)
    }
  })

  return pump(stream, throughv.obj(decodeRetainedPacket))
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

  var pipeline = this._getPipeline()

  var clientSubKey = clientKey + client.id
  var that = this

  var toStore = {}
  var offlines = []
  var count = 0
  var published = 0
  var errored = null

  for (var i = 0; i < subs.length; i++) {
    var sub = subs[i]
    toStore[sub.topic] = sub.qos
    if (sub.qos > 0) {
      offlines.push(sub.topic)
      count++
      // TODO don't wait the client an extra tick
      that._waitFor(client, sub.topic, finish)
    }
  }

  pipeline.sadd(subsKey, offlines)
  pipeline.sadd(clientsKey, client.id)
  pipeline.hmset(clientSubKey, toStore, finish)

  this._addedSubscriptions(client, subs)

  function finish () {
    published++
    if (published === count + 1 && !errored) {
      cb(null, client)
    }
  }
}

RedisPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
    return
  }

  var clientSubKey = clientKey + client.id

  var that = this
  var pipeline = this._getPipeline()
  var published = 0
  var count = 0
  var errored = false

  for (var i = 0; i < subs.length; i++) {
    that._waitFor(client, subs[i], finish)
    count++
  }

  pipeline.srem(subsKey, subs) // TODO matcher.match should be checked

  pipeline.hdel(clientSubKey, subs)

  this._removedSubscriptions(client, subs.map(toSub))

  pipeline.exists(clientSubKey, function checkAllSubsRemoved (err, subCount) {
    if (err) {
      errored = true
      return cb(err)
    }
    if (subCount === 0) {
      pipeline.srem(clientsKey, client.id)
    }
  })

  finish()

  function finish () {
    published++
    if (published === count + 1 && !errored) {
      cb(null, client)
    }
  }
}

function toSub (topic) {
  return {
    topic: topic
  }
}

RedisPersistence.prototype.subscriptionsByClient = function (client, cb) {
  var pipeline = this._getPipeline()

  var clientSubKey = clientKey + client.id

  pipeline.hgetall(clientSubKey, function returnSubs (err, subs) {
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
  var pipeline = this._getPipeline()
  var clientsCount = -1
  var subsCount = -1

  pipeline.scard(clientsKey, function countOfflineClients (err, count) {
    if (err) {
      return cb(err)
    }

    clientsCount = parseInt(count) || 0

    if (subsCount >= 0) {
      cb(null, subsCount, clientsCount)
    }
  })

  pipeline.scard(subsKey, function countSubscriptions (err, count) {
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
  var pipeline = that._getPipeline()

  var splitStream = through.obj(split)

  var hgetallStream = throughv.obj(function getStream (clientId, enc, cb) {
    var clientSubKey = clientKey + clientId
    pipeline.hgetall(clientSubKey, function clientHash (err, hash) {
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
      splitStream.emit('error', err)
    } else {
      splitStream.write(clientIds)
      splitStream.end()
    }
  })

  pump(splitStream, hgetallStream, function pumpStream (err) {
    if (that._destroyed) {
      return
    }

    if (err) {
      that.emit('error', err)
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
  var listKey = 'outgoing:' + sub.clientId
  var key = listKey + ':' + packet.brokerId + ':' + packet.brokerCounter

  var pipeline = this._getPipeline()
  pipeline.rpush(listKey, key)
  pipeline.set(key, msgpack.encode(new Packet(packet)), cb)
}

function updateWithClientData (that, client, packet, cb) {
  var prekey = 'outgoing:' + client.id + ':' + packet.brokerId + ':' + packet.brokerCounter
  var postkey = 'outgoing-id:' + client.id + ':' + packet.messageId
  var encoded = msgpack.encode(packet)

  var pipeline = that._getPipeline()
  pipeline.set(prekey, encoded)
  pipeline.set(postkey, encoded, function setPostKey (err, result) {
    if (err) { return cb(err, client, packet) }

    if (result !== 'OK') {
      cb(new Error('no such packet'), client, packet)
    } else {
      cb(null, client, packet)
    }
  })
}

function augmentWithBrokerData (that, client, packet, cb) {
  var postkey = 'outgoing-id:' + client.id + ':' + packet.messageId
  that._getPipeline().getBuffer(postkey, function augmentBrokerData (err, buf) {
    if (err) {
      return cb(err)
    }

    if (!buf) {
      return cb(new Error('no such packet'))
    }

    var decoded = msgpack.decode(buf)
    packet.brokerId = decoded.brokerId
    packet.brokerCounter = decoded.brokerCounter
    cb(null)
  })
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
  var listKey = 'outgoing:' + client.id
  var key = 'outgoing-id:' + client.id + ':' + packet.messageId

  this._getPipeline().getBuffer(key, function clearMessageId (err, buf) {
    if (err) {
      return cb(err)
    }

    if (!buf) {
      return cb()
    }

    var packet = msgpack.decode(buf)
    var prekey = listKey + ':' + packet.brokerId + ':' + packet.brokerCounter

    var pipeline = that._getPipeline()
    pipeline.del(key)
    pipeline.del(prekey)
    pipeline.lrem(listKey, 0, prekey, function lremKey (err) {
      cb(err, packet)
    })
  })
}

function split (keys, enc, cb) {
  for (var i = 0, l = keys.length; i < l; i++) {
    this.push(keys[i])
  }
  cb()
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
  this._getPipeline().set(key, msgpack.encode(newp), cb)
}

RedisPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  var key = 'incoming:' + client.id + ':' + packet.messageId
  this._getPipeline().getBuffer(key, function decodeBuffer (err, buf) {
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
  this._getPipeline().del(key, cb)
}

RedisPersistence.prototype.putWill = function (client, packet, cb) {
  var key = 'will:' + this.broker.id + ':' + client.id
  packet.clientId = client.id
  packet.brokerId = this.broker.id
  this._getPipeline().rpush(willKey, key)
  this._getPipeline().setBuffer(key, msgpack.encode(packet), encodeBuffer)

  function encodeBuffer (err) {
    cb(err, client)
  }
}

RedisPersistence.prototype.getWill = function (client, cb) {
  var key = 'will:' + this.broker.id + ':' + client.id
  this._getPipeline().getBuffer(key, function getWillForClient (err, packet) {
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
  var pipeline = this._getPipeline()
  pipeline.lrem(willKey, 0, key)
  pipeline.getBuffer(key, function getClientWill (err, packet) {
    if (err) { return cb(err) }

    if (packet) {
      result = msgpack.decode(packet)
    }
  })

  pipeline.del(key, function deleteWill (err) {
    cb(err, result, client)
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
