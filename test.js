'use strict'

var test = require('tape').test
var persistence = require('./')
var Redis = require('ioredis')
var abs = require('aedes-persistence/abstract')
var db = new Redis()

db.on('connect', function() {
  db.connector.stream.unref()
})

abs({
  test: test,
  persistence: function () {
    db.flushall()
    return persistence()
  }
})
