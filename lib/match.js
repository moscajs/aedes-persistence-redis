'use strict'

var Readable = require('stream').Readable
var util = require('util')
var parse = require('fast-json-parse')

function MatchStream (opt) {
  Readable.call(this, opt)

  this.opt = opt
  this.opt.count = this.opt.count || 10
  this.results = false

  this.key = opt.key
  this.getKeys()
}

util.inherits(MatchStream, Readable)

MatchStream.prototype.getKeys = function () {
  var self = this
  this.opt.redis.hkeys(this.key, function getKeys (err, topics) {
    if (err) {
      self.emit('error', err)
    } else {
      self.results = topics
      self.emit('ready')
    }
  })
}

MatchStream.prototype._read = function () {
  if (this.results === false) {
    return this.once('ready', this._read)
  }

  var results = this.results
  this.results = null

  this.push(results)
}

module.exports = MatchStream
