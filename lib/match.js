'use strict'

var Readable = require('stream').Readable
var util = require('util')
var fs = require('fs')
var path = require('path')
var lua = fs.readFileSync(path.join(__dirname, 'cursor.lua'))

function MatchStream (opt) {
  Readable.call(this, opt)

  this.opt = opt
  this.results = false

  this.scan()
}

util.inherits(MatchStream, Readable)

MatchStream.prototype.scan = function () {
  var self = this

  this.opt.redis.eval(lua, 0, this.opt.match, function (err, res) {
    if (err) {
      self.emit('error', err)
      return
    }

    try {
      if (res !== '{}') {
        self.results = JSON.parse(res)
      } else {
        self.results = null
      }

      self.emit('ready')
    } catch (e) {
      self.emit('error', e)
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
