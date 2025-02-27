const test = require('node:test')
const persistence = require('./persistence')
const Redis = require('ioredis')
const mqemitterRedis = require('mqemitter-redis')
const abs = require('aedes-cached-persistence/abstract')

function unref () {
  this.connector.stream.unref()
}

function sleep (sec) {
  return new Promise(resolve => setTimeout(resolve, sec * 1000))
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

db.on('error', e => {
  console.trace(e)
})

function buildEmitter () {
  const emitter = mqemitterRedis()
  emitter.subConn.on('connect', unref)
  emitter.pubConn.on('connect', unref)

  return emitter
}

function clusterPersistence (cb) {
  const slaves = db.nodes('master')
  Promise.all(slaves.map((node) => {
    return node.flushdb().catch(err => {
      console.error('flushRedisKeys-error:', err)
    })
  })).then(() => {
    const conn = new Redis.Cluster(nodes)

    conn.on('error', e => {
      console.trace(e)
    })

    conn.on('ready', () => {
      cb(null, persistence({
        conn,
        cluster: true
      }))
    })
  })
}

db.on('ready', () => {
  abs({
    test,
    buildEmitter,
    persistence: clusterPersistence,
    waitForReady: true
  })
})

sleep(10).then(() => process.exit(0))
