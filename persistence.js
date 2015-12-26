'use strict'

var Packet = require('aedes-packet')
var Redis = require('ioredis')
var through = require('through2')
var throughv = require('throughv')
var msgpack = require('msgpack-lite')

function RedisPersistence (opts) {
  if (!(this instanceof RedisPersistence)) {
    return new RedisPersistence(opts)
  }

  this._db = new Redis(opts)

  var that = this
  this._decodeAndAugment = function decodeAndAugment (chunk, enc, cb) {
    that._db.getBuffer(chunk, function (err, result) {
      var decoded;
      if (result) {
        decoded = msgpack.decode(result)
      }
      cb(err, decoded)
    })
  }
}

RedisPersistence.prototype.storeRetained = function (packet, cb) {
  var key = 'retained:' + packet.topic
  if (packet.payload.length === 0) {
    this._db.del(key, cb)
  } else {
    this._db.set(key, msgpack.encode(packet), cb)
  }
}

function splitArray (keys, enc, cb) {
  for (var i = 0; i < keys.length; i++) {
    this.push(keys[i])
  }
  cb()
}

RedisPersistence.prototype.createRetainedStream = function (pattern) {
  return this._db.scanStream({
    match: 'retained:' + pattern.split(/[#+]/)[0] + '*',
    count: 100
  }).pipe(through.obj(splitArray))
    .pipe(throughv.obj(this._decodeAndAugment))
}

RedisPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  cb(null, client)
}

RedisPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
  cb(null, client)
}

RedisPersistence.prototype.subscriptionsByClient = function (client, cb) {
  cb(null, [], client)
}

RedisPersistence.prototype.countOffline = function (cb) {
  return cb(null, 42, 42)
}

RedisPersistence.prototype.subscriptionsByTopic = function (pattern, cb) {
  cb(null, [])
}

RedisPersistence.prototype.cleanSubscriptions = function (client, cb) {
  cb(null, client)
}

RedisPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  cb(null)
}

RedisPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
  cb(new Error('no such packet'), client, packet)
}

RedisPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
  cb(new Error('no such packet'))
}

RedisPersistence.prototype.outgoingStream = function (client) {
  return null
}

RedisPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  cb(null)
}

RedisPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  cb(err, null)
}

RedisPersistence.prototype.incomingDelPacket = function (client, packet, cb) {
  cb(null)
}

RedisPersistence.prototype.putWill = function (client, packet, cb) {
  cb(null, client)
}

RedisPersistence.prototype.getWill = function (client, cb) {
  cb(null, null, client)
}

RedisPersistence.prototype.delWill = function (client, cb) {
  cb(null, null, client)
}

RedisPersistence.prototype.streamWill = function (brokers) {
  return null
}

RedisPersistence.prototype.destroy = function (cb) {
  this._db.disconnect()
  if (cb) {
    cb(null)
  }
}

module.exports = RedisPersistence
