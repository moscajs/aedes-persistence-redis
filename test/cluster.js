const test = require('node:test')
const persistence = require('../persistence.js')
const Redis = require('ioredis')
const mqemitterRedis = require('mqemitter-redis')
const abs = require('aedes-persistence/abstract')
const { once } = require('node:events')

const nodes = [
  { host: 'localhost', port: 6378 },
  { host: 'localhost', port: 6380 },
  { host: 'localhost', port: 6381 },
  { host: 'localhost', port: 6382 },
  { host: 'localhost', port: 6383 },
  { host: 'localhost', port: 6384 }
]

function sleep (sec) {
  return new Promise(resolve => setTimeout(resolve, sec * 1000))
}

function setUpPersistence (t, id, persistenceOpts) {
  const emitter = mqemitterRedis()
  const instance = persistence(persistenceOpts)
  instance.broker = toBroker(id, emitter)
  t.diagnostic(`instance ${id} created`)
  return { instance, emitter, id }
}

function cleanUpPersistence (t, { instance, emitter, id }) {
  instance.destroy()
  emitter.close()
  t.diagnostic(`instance ${id} destroyed`)
}

function toBroker (id, emitter) {
  return {
    id,
    publish: emitter.emit.bind(emitter),
    subscribe: emitter.on.bind(emitter),
    unsubscribe: emitter.removeListener.bind(emitter)
  }
}
function unref () {
  this.connector.stream.unref()
}

async function createDB () {
  const db = new Redis.Cluster(nodes)
  await once(db, 'connect')
  const dbNodes = db.nodes('master')
  await Promise.all(dbNodes.map(node => { return node.flushdb() }))
  return db
}

async function cleanDB () {
  const db = await createDB()
  db.disconnect()
}

function makeBuildEmitter (opts) {
  return function buildEmitter (opts) {
    const emitter = mqemitterRedis(opts)
    emitter.subConn.on('connect', unref)
    emitter.pubConn.on('connect', unref)
    return emitter
  }
}
function makePersistence (opts = {}) {
  return async function build () {
    await cleanDB()
    opts.cluster = nodes
    const instance = persistence(opts)
    // make intance.destroy close the broker as well
    const oldDestroy = instance.destroy.bind(instance)
    instance.destroy = (cb) => {
      oldDestroy(() => {
        instance.broker.mq.close(cb)
      })
    }
    return instance
  }
}

// testing starts here
async function doTest () {
  test('external Redis conn', async t => {
    t.plan(1)
    const externalRedis = await createDB(t)
    const p = setUpPersistence(t, '1', {
      conn: externalRedis
    })
    await once(p.instance, 'ready')
    t.assert.ok(true, 'instance ready')
    t.diagnostic('instance ready')
    externalRedis.disconnect()
    t.diagnostic('redis disconnected')
    cleanUpPersistence(t, p)
  })

  abs({
    test,
    buildEmitter: makeBuildEmitter(),
    persistence: makePersistence(),
    waitForReady: true
  })
  // make sure everything cleans up nicely
  await sleep(4)
}
doTest()
