const test = require('tape').test
const persistence = require('../')
const Redis = require('ioredis')
const mqemitterRedis = require('mqemitter-redis')
const abs = require('aedes-cached-persistence/abstract')

function unref () {
  this.connector.stream.unref()
}

const nodes = [
  { host: 'localhost', port: 6378 },
  { host: 'localhost', port: 6380 },
  { host: 'localhost', port: 6381 },
  { host: 'localhost', port: 6382 },
  { host: 'localhost', port: 6383 },
  { host: 'localhost', port: 6384 }
]

const db = new Redis.Cluster(nodes)

db.on('ready', function () {
  abs({
    test,
    buildEmitter () {
      const emitter = mqemitterRedis()
      emitter.subConn.on('connect', unref)
      emitter.pubConn.on('connect', unref)

      return emitter
    },
    persistence (cb) {
      const slaves = db.nodes('master')
      Promise.all(slaves.map(function (node) {
        return node.flushdb().catch(err => {
          console.error('flushRedisKeys-error:', err)
        })
      })).then(() => {
        cb(null, persistence({
          cluster: new Redis.Cluster(nodes)
        }))
      })
    },
    waitForReady: true
  })

  test.onFinish(() => {
    process.exit(0)
  })
})
