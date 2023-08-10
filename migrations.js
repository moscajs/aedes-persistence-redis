async function from9to10 (db, cb) {
  // move retained messages from hash to keys
  const RETAINEDKEY = 'retained'
  function retainedKey (topic) {
    return `${RETAINEDKEY}:${encodeURIComponent(topic)}`
  }

  // get all topics
  db.hkeys(RETAINEDKEY, function (err, topics) {
    if (err) {
      return cb(err)
    }

    Promise.all(topics.map(t => {
      return new Promise((resolve, reject) => {
        // get packet payload
        db.hgetBuffer(RETAINEDKEY, t, function (err, payload) {
          if (err) {
            return reject(err)
          }
          // set packet with new format
          db.set(retainedKey(t), payload, function (err) {
            if (err) {
              return reject(err)
            }
            // remove old packet
            db.hdel(RETAINEDKEY, t, function (err) {
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

module.exports = {
  from9to10
}
