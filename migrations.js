async function from9to10 (db, cb) {
  // move retained messages from hash to keys
  const RETAINEDKEY = 'retained'
  function retainedKey (topic) {
    return `${RETAINEDKEY}:${encodeURIComponent(topic)}`
  }

  // get all topics
  db.hkeys(RETAINEDKEY, (err, topics) => {
    if (err) {
      return cb(err)
    }

    Promise.all(topics.map(t => {
      return new Promise((resolve, reject) => {
        // get packet payload
        db.hgetBuffer(RETAINEDKEY, t, (err, payload) => {
          if (err) {
            return reject(err)
          }
          // set packet with new format
          db.set(retainedKey(t), payload, (err) => {
            if (err) {
              return reject(err)
            }
            // remove old packet
            db.hdel(RETAINEDKEY, t, (err) => {
              if (err) {
                return reject(err)
              }
              resolve()
            })
          })
        })
      })
    })).then(() => {
      cb(null)
    }).catch(err => {
      cb(err)
    })
  })
}

// Removes the legacy v11 incoming keys reachable from a single connection.
// Returns how many were removed.
async function removeLegacyIncoming (conn) {
  // ioredis prepends keyPrefix to key arguments, but SCAN takes a pattern, not
  // a key — so the pattern needs the prefix added by hand, and the keys SCAN
  // returns need it stripped again before they go back through a command.
  const keyPrefix = conn.options.keyPrefix || ''
  // clientIds are URI-encoded, so `:` becomes `%3A`: only the old
  // `incoming:<clientId>:<messageId>` keys carry a second colon. A v12
  // `incoming:<clientId>` hash can never match, so a live broker's data is
  // never even looked at.
  const match = `${keyPrefix}incoming:*:*`
  let removed = 0
  let cursor = '0'

  do {
    const [next, keys] = await conn.scan(cursor, 'MATCH', match, 'COUNT', 1000)
    cursor = next

    if (keys.length === 0) {
      continue
    }

    const names = keys.map(key => key.slice(keyPrefix.length))
    // Belt and braces: the pattern already excludes v12 hashes, but a key that
    // is not a string was never written by this library, so leave it alone.
    const types = await conn.pipeline(names.map(name => ['type', name])).exec()
    const legacy = names.filter((name, i) => {
      const [err, type] = types[i]
      if (err) {
        throw err
      }
      return type === 'string'
    })

    if (legacy.length > 0) {
      // DEL, not UNLINK: UNLINK needs Redis 4.0 and the cluster this is tested
      // against is older.
      const deleted = await conn.pipeline(legacy.map(name => ['del', name])).exec()
      for (const [err] of deleted) {
        if (err) {
          throw err
        }
      }
      removed += legacy.length
    }
  } while (cursor !== '0')

  return removed
}

// v12 moved incoming (QoS 2) packets from one string key per messageId
// (`incoming:<clientId>:<messageId>`) into one hash per client
// (`incoming:<clientId>`), so that `cleanIncoming` is a single DEL.
// The old keys are unreachable afterwards; this removes them.
//
// Run it only once every broker is on v12. While a v11 broker is still
// serving, those keys hold live QoS 2 dedup state, and removing them mid-flight
// causes the duplicate delivery this schema change exists to prevent.
//
// Re-running is safe. Accepts a Redis or a Redis.Cluster connection: a cluster
// is scanned on each master, because a cluster-wide SCAN is a keyless command
// and ioredis routes it to one arbitrary node, which would silently leave the
// other shards untouched.
//
// Calls back with the number of keys removed.
async function from11to12 (db, cb) {
  let removed = 0
  try {
    let connections = [db]
    if (typeof db.nodes === 'function') {
      connections = db.nodes('master')
      if (connections.length === 0) {
        throw new Error('no master nodes available, is the cluster connection ready?')
      }
    }
    for (const conn of connections) {
      removed += await removeLegacyIncoming(conn)
    }
  } catch (err) {
    return cb(err)
  }
  // outside the try: a throw from cb is the caller's, not a migration failure
  cb(null, removed)
}

module.exports = {
  from9to10,
  from11to12
}
