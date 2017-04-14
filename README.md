# aedes-persistence-redis&nbsp;&nbsp;[![Build Status](https://travis-ci.org/mcollina/aedes-persistence-redis.svg)](https://travis-ci.org/mcollina/aedes-persistence-redis)

[Aedes][aedes] [persistence][persistence], backed by [Redis][redis].

See [aedes-persistence][persistence] for the full API, and [Aedes][aedes] for usage.

## Install

```
npm i aedes aedes-persistence-redis --save
```

## API

<a name="constructor"></a>
### aedesPersistenceRedis([opts])

Creates a new instance of aedes-persistence-redis.
It takes all the same options of [ioredis](http://npm.im/ioredis),
which is used internally to connect to Redis.

This constructor creates two connections to Redis.

Example:

```js
aedesPersistenceRedis({
  port: 6379,          // Redis port
  host: '127.0.0.1',   // Redis host
  family: 4,           // 4 (IPv4) or 6 (IPv6)
  password: 'auth',
  db: 0
})
```

### Updates in v3.x
v2 used scan stream with Lua which would slow down the connections. Also it would lead to high CPU usage on the Redis server because most of the processing happened on the Redis server. v3 follows the
Mosca's approach of keeping maps of keys and then accessing them rather than doing any sort of scanning with wildcards.

Changes made :-
- `retained` key is a Redis Hashmap of all retained keys.
- `sub:client` key is a Redis list of all subscription keys
- `will` key is a Redis list for all will keys
- `outgoing: + clientId` key will contain keys for all outgoing messages

## License

MIT

[aedes]: https://github.com/mcollina/aedes
[persistence]: https://github.com/mcollina/aedes-persistence
[redis]: http://redis.io
