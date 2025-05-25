'use strict'

const { CallBackPersistence } = require('aedes-persistence/callBackPersistence.js')
const AsyncPersistence = require('./asyncPersistence.js')
const asyncInstanceFactory = (opts) => new AsyncPersistence(opts)
module.exports = (opts) => new CallBackPersistence(asyncInstanceFactory, opts)
