'use strict'

var Readable = require('stream').Readable
var util = require('util')
var parse = require('fast-json-parse')

function MatchStream (opt) {
  Readable.call(this, opt)

  this.opt = opt
  this.results = false

  if (opt.retained) {
    this.getRetainedKeys()
  } else {
    this.scan()
  }

}

util.inherits(MatchStream, Readable)

MatchStream.prototype.scan = function () {
  var self = this

  this.opt.redis.executeLua(0, this.opt.match, function evalLua (err, res) {
    handleEvalLua(err, res, self)
  })
}

function handleEvalLua (err, res, self) {
  if (err) {
    self.emit('error', err)
    return
  }

  if (res !== '{}') {
    var results = parse(res)

    if (results.err) {
      self.emit('error', results.err)
      return
    }
    self.results = results.value
  } else {
    self.results = null
  }

  self.emit('ready')
}

MatchStream.prototype.getRetainedKeys = function() {
  var self = this
  this.opt.redis.hkeys('retained', function getKeys(err, topics) {
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
