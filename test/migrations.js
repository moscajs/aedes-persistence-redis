const test = require('node:test')
const Redis = require('ioredis')
const { once } = require('node:events')
const { from11to12 } = require('../migrations.js')

const clusterNodes = [
  { host: 'localhost', port: 6378 },
  { host: 'localhost', port: 6380 },
  { host: 'localhost', port: 6381 },
  { host: 'localhost', port: 6382 },
  { host: 'localhost', port: 6383 },
  { host: 'localhost', port: 6384 }
]

function runMigration (db) {
  return new Promise((resolve, reject) => {
    from11to12(db, (err, removed) => {
      if (err) {
        reject(err)
      } else {
        resolve(removed)
      }
    })
  })
}

// v11 layout: one string key per message. v12 layout: one hash per client.
// `unrelated` must survive whatever happens.
const legacyKeys = ['incoming:clientA:1', 'incoming:clientA:2', 'incoming:clientB:7']

async function seed (db) {
  for (const key of legacyKeys) {
    await db.set(key, `legacy-${key}`)
  }
  await db.hset('incoming:clientC', '1', 'kept-1')
  await db.hset('incoming:clientC', '2', 'kept-2')
  await db.set('retained:hello', 'kept-retained')
}

async function assertMigrated (t, db) {
  for (const key of legacyKeys) {
    t.assert.equal(await db.exists(key), 0, `${key} removed`)
  }
  t.assert.deepEqual(await db.hgetall('incoming:clientC'),
    { 1: 'kept-1', 2: 'kept-2' }, 'v12 hash untouched')
  t.assert.equal(await db.get('retained:hello'), 'kept-retained', 'unrelated key untouched')
}

test('from11to12 removes legacy incoming keys and leaves the rest alone', async t => {
  const db = new Redis()
  await once(db, 'connect')
  await db.flushall()
  await seed(db)

  t.assert.equal(await runMigration(db), legacyKeys.length, 'reports what it removed')
  await assertMigrated(t, db)

  // idempotent
  t.assert.equal(await runMigration(db), 0, 'second run removes nothing')
  await assertMigrated(t, db)

  db.disconnect()
})

test('from11to12 is a no-op on an empty database', async t => {
  const db = new Redis()
  await once(db, 'connect')
  await db.flushall()

  t.assert.equal(await runMigration(db), 0, 'nothing to remove')

  db.disconnect()
})

test('from11to12 honours a connection keyPrefix', async t => {
  const plain = new Redis()
  await once(plain, 'connect')
  await plain.flushall()

  // ioredis does not prefix a SCAN MATCH pattern, and re-prefixes the keys SCAN
  // hands back, so a prefixed connection used to migrate nothing at all.
  const db = new Redis({ keyPrefix: 'pfx:' })
  await once(db, 'connect')
  await seed(db)
  await plain.set('incoming:outside:1', 'other-namespace')

  t.assert.equal(await runMigration(db), legacyKeys.length, 'found the prefixed keys')
  await assertMigrated(t, db)
  t.assert.equal(await plain.get('incoming:outside:1'), 'other-namespace',
    'key outside the prefix untouched')

  plain.disconnect()
  db.disconnect()
})

test('from11to12 covers every master node of a cluster', async t => {
  const db = new Redis.Cluster(clusterNodes)
  await once(db, 'ready')
  const masters = db.nodes('master')
  await Promise.all(masters.map(node => node.flushdb()))

  // A cluster-wide SCAN is keyless, so ioredis sends it to one arbitrary node.
  // Enough clients to land on every master, so a single-node scan cannot pass.
  const clientIds = Array.from({ length: 300 }, (_, i) => `clusterClient${i}`)
  for (const id of clientIds) {
    await db.set(`incoming:${id}:1`, 'legacy')
  }
  await db.hset('incoming:keepMe', '1', 'kept')

  t.assert.equal(await runMigration(db), clientIds.length, 'removed every legacy key')
  for (const id of clientIds) {
    t.assert.equal(await db.exists(`incoming:${id}:1`), 0, `incoming:${id}:1 removed`)
  }
  t.assert.deepEqual(await db.hgetall('incoming:keepMe'), { 1: 'kept' }, 'v12 hash untouched')

  await Promise.all(masters.map(node => node.flushdb()))
  db.disconnect()
})
