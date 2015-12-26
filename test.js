'use strict'

var test = require('tape').test
var redis = require('./')
var abs = require('aedes-persistence/abstract')

abs({
  test: test,
  persistence: redis
})
