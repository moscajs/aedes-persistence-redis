# Upgrade

## x.x.x to 9.x.x

The database schema has changed between 8.x.x to 9.x.x. 

Start with a clean database if you migrate from x.x.x to 9.x.x

# x.x.x to 10.x.x

The database schema has changed between 9.x.x to 10.x.x **IF YOU ARE USING CLUSTERS**.

Start with a clean database **IF YOU ARE USING CLUSTERS** migrate from x.x.x to 10.x.x or use `migrations.js` `from9to10` function.

## x.x.x to 12.x.x

The incoming (QoS 2) key schema has changed between 11.x.x and 12.x.x.

Incoming packets used to live in one string key per message
(`incoming:<clientId>:<messageId>`) and now live in one hash per client
(`incoming:<clientId>`), so that `cleanIncoming` is a single `DEL`.

Incoming packets are in-flight state only, so nothing durable is lost. The old
keys are unreachable after the upgrade: either start with a clean database or
use `migrations.js` `from11to12` to remove them.

The advisory is only closed once the broker calls `cleanIncoming` on a
clean-session connect. Brokers feature-detect it, so one that does not call it
takes the schema change without the fix.

### Upgrading brokers that share a Redis

Upgrade every broker at once. The two layouts are disjoint, so during a rolling
restart a v11 broker and a v12 broker cannot see each other's QoS 2 dedup
state: a client whose `PUBLISH` and `PUBREL` land on different versions gets a
`no such packet`. In-flight QoS 2 messages do not survive the cut either way.

### Running the migration

```js
const { from11to12 } = require('aedes-persistence-redis/migrations.js')

from11to12(db, (err, removed) => {
  console.log(err || `removed ${removed} legacy keys`)
})
```

Run it only once every broker is on v12. While a v11 broker is still serving,
those keys hold live dedup state, and removing them mid-flight causes the
duplicate delivery this schema change exists to prevent.

Re-running is safe. `db` may be a `Redis` or a `Redis.Cluster` connection — a
cluster is scanned on every master node.

### Downgrading

There is no `from12to11`. A v11 broker neither reads nor deletes
`incoming:<clientId>` hashes, so downgrading orphans them; drop them with
`DEL` on the keys matching `incoming:*` that are hashes, or start from a
clean database.
