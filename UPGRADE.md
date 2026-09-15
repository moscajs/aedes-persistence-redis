# Upgrade

## x.x.x to 9.x.x

The database schema has changed between 8.x.x to 9.x.x. 

Start with a clean database if you migrate from x.x.x to 9.x.x

# x.x.x to 10.x.x

The database schema has changed between 9.x.x to 10.x.x **IF YOU ARE USING CLUSTERS**.

Start with a clean database **IF YOU ARE USING CLUSTERS** migrate from x.x.x to 10.x.x or use `migrations.js` `from9to10` function.

# x.x.x to 12.x.x

The incoming (QoS 2) key schema has changed between 11.x.x and 12.x.x.

Incoming packets used to live in one string key per message
(`incoming:<clientId>:<messageId>`) and now live in one hash per client
(`incoming:<clientId>`), so that `cleanIncoming` is a single `DEL`.

Incoming packets are in-flight state only, so nothing durable is lost. The old
keys are unreachable after the upgrade: either start with a clean database or
use `migrations.js` `from11to12` to remove them.
