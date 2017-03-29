'use strict'

var Redis = require('ioredis')
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
var offlineClientsCountKey = 'counter:offline:clients'
var offlineSubscriptionsCountKey = 'counter:offline:subscriptions'
var subscriptionsKey = 'sub:client'
var willKey = 'will'
var retainedKey = 'retained'
var outgoingKey = 'outgoing:'

function RedisPersistence (opts) {
  if (!(this instanceof RedisPersistence)) {
    return new RedisPersistence(opts)
  }

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
    this._db.hdel(retainedKey, packet.topic, cb)
  } else {
    this._db.hset(retainedKey, packet.topic, msgpack.encode(packet), cb)
  }
}

RedisPersistence.prototype.createRetainedStream = function (pattern) {
  var that = this
  var qlobber = new Qlobber(qlobberOpts)
  qlobber.add(pattern, true)

  var stream = through.obj(that._getChunk)

  this._db.hkeys(retainedKey, function getKeys (err, keys) {
    if (err) {
      stream.emit('error', err)
    } else {
      for (var i = 0, l = keys.length; i < l; i++) {
        if (qlobber.match(keys[i]).length > 0) {
          stream.write(keys[i])
        }
      }
      stream.end()
      stream = null
    }
  })

  return stream.pipe(throughv.obj(decodeRetainedPacket))
}

function decodeRetainedPacket (chunk, enc, cb) {
  cb(null, msgpack.decode(chunk))
}

function Sub (clientId, topic, qos) {
  this.clientId = clientId
  this.topic = topic
  this.qos = qos
}

RedisPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  if (!this.ready) {
    this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
    return
  }

  var multi = this._db.multi()

  var clientSubKey = 'client:sub:' + client.id
  var that = this

  var toStore = {}

  var count = 0
  var published = 0
  var errored = null

  multi.exists(clientSubKey)

  for (var i = 0; i < subs.length; i++) {
    var sub = subs[i]
    toStore[sub.topic] = sub.qos

    if (sub.qos > 0) {
      var subClientKey = 'sub:client:' + sub.topic
      var encoded = msgpack.encode(new Sub(client.id, sub.topic, sub.qos))
      multi.rpush(subscriptionsKey, subClientKey)
      multi.hset(subClientKey, client.id, encoded)
      count++
      that._waitFor(client, sub.topic, finish)
    }
  }

  multi.hmset(clientSubKey, toStore)

  this._addedSubscriptions(client, subs)

  multi.exec(function execMulti (err, results) {
    if (err) {
      errored = true
      return cb(err, client)
    }

    var existed = results.length > 0 && results[0][1] > 0
    var pipeline = that._getPipeline()
    if (!existed) {
      pipeline.incr(offlineClientsCountKey)
    }

    pipeline.incrby(offlineSubscriptionsCountKey, count, finish)
  })

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

  var clientSubKey = 'client:sub:' + client.id

  var that = this
  var multi = this._db.multi()
  multi.hgetall(clientSubKey)
  var published = 0
  var count = 0
  var errored = false

  for (var i = 0; i < subs.length; i++) {
    var topic = subs[i]
    var subClientKey = 'sub:client:' + topic
    multi.lrem(subscriptionsKey, 1, subClientKey)
    multi.hdel(subClientKey, client.id)
    that._waitFor(client, topic, finish)
    count++
    multi.hdel(clientSubKey, topic)
  }

  this._removedSubscriptions(client, subs.map(toSub))

  multi.exec(function execMulti (err, results) {
    if (err) {
      errored = true
      return cb(err)
    }

    var skipped = 0
    skipped = getSkipped(results, skipped)
    var pipeline = that._getPipeline()
    pipeline.decrby(offlineSubscriptionsCountKey, subs.length - skipped, finish)
  })

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

function getSkipped (results, skipped) {
  for (var i = 1; i < results.length; i += 3) {
    if (results[i] === '0') {
      skipped++
    }
  }
  return skipped
}

RedisPersistence.prototype.subscriptionsByClient = function (client, cb) {
  var pipeline = this._getPipeline()

  var clientSubKey = 'client:sub:' + client.id

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
  var subsCount = -1
  var clientsCount = -1

  pipeline.get(offlineSubscriptionsCountKey, function countOfflineSubs (err, count) {
    if (err) {
      return cb(err)
    }
    subsCount = parseInt(count)

    if (clientsCount >= 0) {
      cb(null, subsCount, clientsCount)
    }
  })

  pipeline.get(offlineClientsCountKey, function countOfflineSClients (err, count) {
    if (err) {
      return cb(err)
    }

    clientsCount = parseInt(count)

    if (subsCount >= 0) {
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

  var splitStream = through.obj(split)

  var hgetallStream = throughv.obj(function getStream (chunk, enc, cb) {
    var pipeline = that._getPipeline()
    pipeline.hgetallBuffer(chunk, cb)
  }, function emitReady (cb) {
    that.ready = true
    that.emit('ready')
    cb()
  })
  .on('data', function processKeys (all) {
    processKeysForClient(all, that)
  })

  this._db.lrange(subscriptionsKey, 0, 10000, function lrangeResult (err, results) {
    if (err) {
      splitStream.emit('error', err)
    } else {
      splitStream.write(results)
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

function processKeysForClient (all, that) {
  var keys = Object.keys(all)
  var key = ''
  var decoded = null
  var raw = null

  for (var i = 0; i < keys.length; i++) {
    key = keys[i]
    raw = that[key]
    if (!raw) {
      continue
    }
    decoded = msgpack.decode(raw)
    decoded.clientId = key

    that._matcher.add(decoded.topic, decoded)
  }
}

RedisPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  var listKey = 'outgoing:' + sub.clientId
  var key = listKey + ':' + packet.brokerId + ':' + packet.brokerCounter

  var multi = this._db.multi()
  multi.set(key, msgpack.encode(new Packet(packet)))
  multi.rpush(listKey, key)

  multi.exec(cb)
}

function updateWithClientData (that, client, packet, cb) {
  var prekey = 'outgoing:' + client.id + ':' + packet.brokerId + ':' + packet.brokerCounter
  var postkey = 'outgoing-id:' + client.id + ':' + packet.messageId

  var multi = that._db.multi()
  multi.set(postkey, msgpack.encode(packet))
  multi.set(prekey, msgpack.encode(packet))

  multi.exec(function execMulti (err, results) {
    if (err) { return cb(err, client, packet) }

    if (results[0][1] !== 'OK') {
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

    var multi = that._db.multi()
    multi.del(key)
    multi.del(prekey)
    multi.lrem(listKey, 0, prekey)

    multi.exec(function execOps (err) {
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

  this._db.lrange(outgoingKey + client.id, 0, 10000, function lrangeResult (err, results) {
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
  var key = 'will:' + this.broker.id + ':' + client.id
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
  var that = this
  var key = 'sub:client:' + topic

  var stream = through.obj(getStream)

  stream.write(key)
  stream.end()

  return stream.pipe(through.obj(pushClientList))

  function getStream (chunk, enc, cb) {
    var pipeline = that._getPipeline()
    pipeline.hgetall(chunk, cb)
  }
}

function pushClientList (chunk, enc, done) {
  var clients = Object.keys(chunk)
  for (var i = 0; i < clients.length; i++) {
    this.push(clients[i])
  }
  done()
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
