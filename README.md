# aedes-persistence-redis

![.github/workflows/ci.yml](https://github.com/robertsLando/aedes-persistence-redis/workflows/.github/workflows/ci.yml/badge.svg)
[![Dependencies Status](https://david-dm.org/mcollina/aedes-persistence-redis/status.svg)](https://david-dm.org/mcollina/aedes-persistence-redis)
[![devDependencies Status](https://david-dm.org/mcollina/aedes-persistence-redis/dev-status.svg)](https://david-dm.org/mcollina/aedes-persistence-redis?type=dev)
\
[![Known Vulnerabilities](https://snyk.io/test/github/mcollina/aedes-persistence-redis/badge.svg)](https://snyk.io/test/github/mcollina/aedes-persistence-redis)
[![Coverage Status](https://coveralls.io/repos/mcollina/aedes-persistence-redis/badge.svg?branch=master&service=github)](https://coveralls.io/github/mcollina/aedes-persistence-redis?branch=master)
[![NPM version](https://img.shields.io/npm/v/aedes-persistence-redis.svg?style=flat)](https://npm.im/aedes-persistence-redis)
[![NPM downloads](https://img.shields.io/npm/dm/aedes-persistence-redis.svg?style=flat)](https://npm.im/aedes-persistence-redis)

Aedes Persistence, backed by [Redis][redis].

See [aedes-persistence][aedes-persistence] for the full API, and [Aedes][aedes] for usage.

## Install

```sh
npm install aedes aedes-persistence-redis --save
```

## API

### aedesPersistenceRedis([opts])

Creates a new instance of aedes-persistence-redis.
It takes all the same options of [ioredis](https://npm.im/ioredis),
which is used internally to connect to Redis.

This constructor creates two connections to Redis.

Example:

```js
aedesPersistenceRedis({
  port: 6379,          // Redis port
  host: '127.0.0.1',   // Redis host
  family: 4,           // 4 (IPv4) or 6 (IPv6)
  password: 'auth',
  db: 0,
  maxSessionDelivery: 100, // maximum offline messages deliverable on client CONNECT, default is 1000
  packetTTL: function (packet) { // offline message TTL, default is disabled
    return 10 //seconds
  }
})
```

### Changes in v4.x

v4 has changed the subscriptions key schema to enhance performance. Please check [related PR](https://github.com/mcollina/aedes-persistence-redis/pull/31) for more details.

## License

MIT

[aedes]: https://npm.im/aedes
[aedes-persistence]: https://npm.im/aedes-persistence
[redis]: https://redis.io
