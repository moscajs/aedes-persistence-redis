'use strict'

var Readable = require('stream').Readable
var util = require('util')

function RangeStream (opt) {
  Readable.call(this, opt)

  this.opt = opt
  this.opt.count = this.opt.count || 10
  this.results = false

  this.range()
}

util.inherits(RangeStream, Readable)

RangeStream.prototype.range = function () {
  var self = this

  this.opt.redis.lrange(this.opt.key, 0, 10000, function lrangeResult (err, results) {
    handlePacket(err, results, self)
  })
}

function handlePacket (err, res, self) {
  if (err) {
    self.emit('error', err)
    return
  }
  console.log('========', res)
  if (res.length) {
    self.results = res
  } else {
    self.results = null
  }

  // if (res !== '{}') {
  //   var results = parse(res)

  //   if (results.err) {
  //     self.emit('error', results.err)
  //     return
  //   }
  //   self.results = results.value
  // } else {
  //   self.results = null
  // }

  self.emit('ready')
}

RangeStream.prototype._read = function () {
  if (this.results === false) {
    return this.once('ready', this._read)
  }

  var results = this.results
  this.results = null

  this.push(results)
}

module.exports = RangeStream
