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

// v12 moved incoming (QoS 2) packets from one string key per messageId
// (`incoming:<clientId>:<messageId>`) into one hash per client
// (`incoming:<clientId>`), so that `cleanIncoming` is a single DEL.
// The old keys are unreachable afterwards; this removes them.
// Incoming packets are in-flight state only, so nothing worth keeping is lost.
// On a cluster, run this once against each master node.
async function from11to12 (db, cb) {
  try {
    let cursor = '0'
    do {
      const [next, keys] = await db.scan(cursor, 'MATCH', 'incoming:*', 'COUNT', 1000)
      cursor = next
      for (const key of keys) {
        // The new per-client keys are hashes; only the old ones are strings.
        if (await db.type(key) === 'string') {
          await db.unlink(key)
        }
      }
    } while (cursor !== '0')
    cb(null)
  } catch (err) {
    cb(err)
  }
}

module.exports = {
  from9to10,
  from11to12
}
