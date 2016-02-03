'use strict'

var Packet = require('aedes-packet')
var Redis = require('ioredis')
var through = require('through2')
var throughv = require('throughv')
var msgpack = require('msgpack-lite')
var pump = require('pump')
var EventEmitter = require('events').EventEmitter
var inherits = require('util').inherits
var Qlobber = require('qlobber').Qlobber
var nextTick = require('process-nextick-args')
var qlobberOpts = {
  separator: '/',
  wildcard_one: '+',
  wildcard_some:  '#'
}
var offlineClientsCountKey = 'counter:offline:clients'
var offlineSubscriptionsCountKey = 'counter:offline:subscriptions'
var newSubTopic = '$SYS/sub/add'
var rmSubTopic = '$SYS/sub/rm'
var subTopic = '$SYS/sub/+'

function RedisPersistence (opts) {
  if (!(this instanceof RedisPersistence)) {
    return new RedisPersistence(opts)
  }

  this._db = new Redis(opts)
  this._pipeline = null

  var that = this
  this._decodeAndAugment = function decodeAndAugment (chunk, enc, cb) {
    that._getPipeline().getBuffer(chunk, function (err, result) {
      var decoded
      if (result) {
        decoded = msgpack.decode(result)
      }
      cb(err, decoded)
    })
  }

  this._matcher = new Qlobber(qlobberOpts)
  this._ready = false
  this._destroyed = false

  this._waiting = {}

  this._onMessage = function onSubMessage (packet, cb) {
    var decoded = msgpack.decode(packet.payload)
    if (packet.topic === newSubTopic) {
      that._matcher.add(decoded.topic, decoded)
    } else if (packet.topic === rmSubTopic) {
      that._matcher
        .match(decoded.topic)
        .filter(matching, decoded)
        .forEach(rmSub, that._matcher)
    }
    var key = decoded.clientId + '-' + decoded.topic
    var waiting = that._waiting[key]
    delete that._waiting[key]
    if (waiting) {
      process.nextTick(waiting)
    }
    cb()
  }
}

function matching (sub) {
  return sub.topic === this.topic
}

function rmSub (sub) {
  this.remove(sub.topic, sub)
}

inherits(RedisPersistence, EventEmitter)

Object.defineProperty(RedisPersistence.prototype, 'broker', {
  get: function () {
    return this._broker
  },
  set: function (broker) {
    this._broker = broker
    broker.subscribe(subTopic, this._onMessage, this._setup.bind(this))
  }
})

RedisPersistence.prototype._getPipeline = function() {
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
  var key = 'retained:' + packet.topic
  if (packet.payload.length === 0) {
    this._db.del(key, cb)
  } else {
    this._db.set(key, msgpack.encode(packet), cb)
  }
}

function checkAndSplit (prefix, pattern) {
  var qlobber = new Qlobber(qlobberOpts)
  qlobber.add(pattern, true)

  // TODO use ctor
  var instance = through.obj(splitArray)

  instance._qlobber = qlobber
  instance._prefix = prefix

  return instance
}

function splitArray (keys, enc, cb) {
  var prefix = this._prefix.length
  for (var i = 0, l = keys.length; i < l; i++) {
    var key = keys[i].slice(prefix)
    if (this._qlobber.match(key).length > 0) {
      this.push(keys[i])
    }
  }
  cb()
}

RedisPersistence.prototype.createRetainedStream = function (pattern) {
  return this._db.scanStream({
    match: 'retained:' + pattern.split(/[#+]/)[0] + '*',
    count: 100
  }).pipe(checkAndSplit('retained:', pattern))
    .pipe(throughv.obj(this._decodeAndAugment))
}

function asKeyValuePair (acc, sub) {
  acc[sub.topic] = sub.qos
  return acc
}

function Sub (clientId, topic, qos) {
  this.clientId = clientId
  this.topic = topic
  this.qos = qos
}

RedisPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  if (!this._ready) {
    this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
    return
  }

  var multi = this._db.multi()

  var clientSubKey = "client:sub:" + client.id
  var that = this
  var broker = this._broker

  var toStore = subs.reduce(asKeyValuePair, {})
  multi.exists(clientSubKey)
  multi.hmset(clientSubKey, toStore)

  var count = 0
  var published = 0
  var errored = null

  subs.forEach(function (sub) {
    if (sub.qos > 0) {
      var subClientKey = 'sub:client:' + sub.topic
      var encoded = msgpack.encode(new Sub(client.id, sub.topic, sub.qos))
      multi.hset(subClientKey, client.id, encoded)
      broker.publish(new Packet({ topic: newSubTopic, payload: encoded }))
      count++
      that._waiting[client.id + '-' + sub.topic] = finish
    }
  })

  multi.exec(function (err, results) {
    if (err) {
      errored = true
      return cb(err, client)
    }

    var existed = results.length > 0 && results[0][1] > 0
    var pipeline = that._getPipeline()
    if (!existed)
      pipeline.incr(offlineClientsCountKey)

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
  if (!this._ready) {
    this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
    return
  }

  var clientSubKey = "client:sub:" + client.id

  var that = this
  var multi = this._db.multi()
  multi.hgetall(clientSubKey)
  var published = 0
  var count = 0
  var errored = false
  var broker = this._broker

  subs.reduce(function (multi, topic) {
    var subClientKey = 'sub:client:' + topic
    multi.hdel(subClientKey, client.id)
    broker.publish(new Packet({ topic: rmSubTopic, payload: msgpack.encode({
      topic: topic,
      clientId: client.id
    })}))
    that._waiting[client.id + '-' + topic] = finish
    count++
    return multi.hdel(clientSubKey, topic)
  }, multi)

  multi.exec(function (err, results) {
    if (err) {
      errored = true
      return cb(err)
    }

    var prev = 0
    var skipped = 0
    for (var i = 1; i < results.length; i += 3) {
      if (results[i] === '0') {
        skipped++
      }
    }
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

RedisPersistence.prototype.subscriptionsByClient = function (client, cb) {
  var pipeline = this._getPipeline()

  var clientSubKey = "client:sub:" + client.id
  var that = this

  pipeline.hgetall(clientSubKey, function (err, subs) {
    var toReturn = Object.keys(subs).map(function (sub) {
      return {
        topic: sub,
        qos: parseInt(this[sub])
      }
    }, subs)
    cb(err, toReturn.length > 0 ? toReturn : null, client)
  })
}

RedisPersistence.prototype.countOffline = function (cb) {
  var pipeline = this._getPipeline()
  var subsCount = -1
  var clientsCount = -1
  pipeline.get(offlineSubscriptionsCountKey, function (err, count) {
    if (err) { return cb(err) }

    subsCount = parseInt(count)

    if (clientsCount >= 0)
      cb(null, subsCount, clientsCount)
  })
  pipeline.get(offlineClientsCountKey, function (err, count) {
    if (err) { return cb(err) }

    clientsCount = parseInt(count)

    if (subsCount >= 0)
      cb(null, subsCount, clientsCount)
  })
}

RedisPersistence.prototype.subscriptionsByTopic = function (topic, cb) {
  if (!this._ready) {
    this.once('ready', this.subscriptionsByTopic.bind(this, topic, cb))
    return this
  }

  var result = this._matcher.match(topic)

  cb(null, result)
}

RedisPersistence.prototype._setup = function () {
  if (this._ready) {
    return
  }

  var that = this
  var prefix = 'sub:client:'

  var scanStream = this._db.scanStream({
    match: prefix + '*',
    count: 100
  })
  var splitStream = through.obj(split)
  var hgetallStream = throughv.obj(function (chunk, enc, cb) {
    var pipeline = that._getPipeline()
    pipeline.hgetallBuffer(chunk, cb)
  }, function (cb) {
    that._ready = true
    that.emit('ready')
    cb()
  })
  .on('data', function (all) {
    Object.keys(all).forEach(insert, all)
  })

  pump(scanStream, splitStream, hgetallStream, function (err) {
    if (that._destroyed) {
      return
    }

    if (err) {
      that.emit('error', err)
    }
  })

  function insert (key) {
    var decoded = msgpack.decode(this[key])
    decoded.clientId = key

    that._matcher.add(decoded.topic, decoded)
  }
}

RedisPersistence.prototype.cleanSubscriptions = function (client, cb) {
  var that = this
  this.subscriptionsByClient(client, function (err, subs, client) {
    if (err || !subs) { return cb(err, client) }
    subs = subs.map(subToTopic)
    that.removeSubscriptions(client, subs, cb)
  })
}

function subToTopic (sub) {
  return sub.topic
}

RedisPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  var key = 'outgoing:' + sub.clientId + ':' + packet.brokerId + ':' + packet.brokerCounter
  this._getPipeline().set(key, msgpack.encode(new Packet(packet)), cb)
}

function updateWithBrokerData (that, client, packet, cb) {
  var prekey = 'outgoing:' + client.id + ':' + packet.brokerId + ':' + packet.brokerCounter
  var postkey = 'outgoing-id:' + client.id + ':' + packet.messageId

  var multi = that._db.multi()
  multi.set(postkey, msgpack.encode(packet))
  multi.set(prekey, msgpack.encode(packet))

  multi.exec(function (err, results) {
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
  that._getPipeline().getBuffer(postkey, function (err, buf) {
    if (err) { return cb(err) }
    if (!buf) { return cb(new Error('no such packet')) }
    var decoded = msgpack.decode(buf)
    packet.brokerId = decoded.brokerId
    packet.brokerCounter = decoded.brokerCounter
    cb(null)
  })
}

RedisPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
  var that = this
  if (packet.brokerId) {
    updateWithBrokerData(this, client, packet, cb)
  } else {
    augmentWithBrokerData(this, client, packet, function (err) {
      if (err) { return cb(err, client, packet) }

      updateWithBrokerData(that, client, packet, cb)
    })
  }
}

RedisPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
  var that = this
  var key = 'outgoing-id:' + client.id + ':' + packet.messageId
  this._getPipeline().getBuffer(key, function (err, buf) {
    if (err) { return cb(err) }
    if (!buf) { return cb(new Error('no such packet')) }

    var packet = msgpack.decode(buf)
    var prekey = 'outgoing:' + client.id + ':' + packet.brokerId + ':' + packet.brokerCounter
    var multi = that._db.multi()
    multi.del(key)
    multi.del(prekey)
    multi.exec(function (err) {
      cb(err, client)
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
  return this._db.scanStream({
    match: 'outgoing:' + client.id + ':*',
    count: 16
  }).pipe(through.obj(split))
    .pipe(throughv.obj(this._decodeAndAugment))
}

RedisPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  var key = 'incoming:' + client.id + ':' + packet.messageId
  var newp = new Packet(packet)
  newp.messageId = packet.messageId
  this._getPipeline().set(key, msgpack.encode(newp), cb)
}

RedisPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  var key = 'incoming:' + client.id + ':' + packet.messageId
  this._getPipeline().getBuffer(key, function (err, buf) {
    if (err) { return cb(err) }
    if (!buf) { return cb(new Error('no such packet')) }
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
  this._getPipeline().setBuffer(key, msgpack.encode(packet), function (err) {
    cb(err, client)
  })
}

RedisPersistence.prototype.getWill = function (client, cb) {
  var key = 'will:' + this.broker.id + ':' + client.id
  this._getPipeline().getBuffer(key, function (err, packet) {
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

  pipeline.getBuffer(key, function (err, packet) {
    if (err) { return cb(err) }

    if (packet) {
      result = msgpack.decode(packet)
    }
  })

  pipeline.del(key, function (err) {
    cb(err, result, client)
  })
}

RedisPersistence.prototype.streamWill = function (brokers) {
  return this._db.scanStream({
    match: 'will:*',
    count: 100
  })
  .pipe(through.obj(function (chunk, enc, cb) {
    for (var i = 0, l = chunk.length; i < l; i++) {
      if (!brokers || !brokers[chunk[i].split(':')[1]]) {
        this.push(chunk[i])
      }
    }
    cb()
  }))
  .pipe(throughv.obj(this._decodeAndAugment))
}

RedisPersistence.prototype.destroy = function (cb) {
  var that = this
  this._destroyed = true
  this.broker.unsubscribe(subTopic, this._onMessage, function () {
    that._db.disconnect()

    if (cb) {
      that._db.on('end', cb)
    }
  })
}

module.exports = RedisPersistence
