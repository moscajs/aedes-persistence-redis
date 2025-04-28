'use strict'

const { Readable } = require('node:stream')
const CachedPersistence = require('aedes-cached-persistence')
const { AsyncRedisPersistence } = require('./asyncPersistence')

class RedisPersistence extends CachedPersistence {
  constructor (opts = {}) {
    super(opts)
    this.asyncPersistence = new AsyncRedisPersistence(opts)
  }

  _setup () {
    if (this.ready) {
      return
    }
    this.asyncPersistence.broker = this.broker
    this.asyncPersistence._trie = this._trie
    this.asyncPersistence.setup()
      .then(() => {
        this.emit('ready')
      })
      .catch(err => {
        this.emit('error', err)
      })
  }

  storeRetained (packet, cb) {
    if (!this.ready) {
      this.once('ready', this.storeRetained.bind(this, packet, cb))
      return
    }
    this.asyncPersistence.storeRetained(packet).then(() => {
      cb(null)
    }).catch(cb)
  }

  createRetainedStream (pattern) {
    return Readable.from(this.asyncPersistence.createRetainedStream(pattern))
  }

  createRetainedStreamCombi (patterns) {
    return Readable.from(this.asyncPersistence.createRetainedStreamCombi(patterns))
  }

  addSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
      return
    }

    const addSubs1 = this.asyncPersistence.addSubscriptions(client, subs)
    // promisify
    const addSubs2 = new Promise((resolve, reject) => {
      this._addedSubscriptions(client, subs, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
    Promise.all([addSubs1, addSubs2])
      .then(() => cb(null, client))
      .catch(err => cb(err, client))
  }

  removeSubscriptions (client, subs, cb) {
    if (!this.ready) {
      this.once('ready', this.removeSubscriptions.bind(this, client, subs, cb))
      return
    }

    const remSubs1 = this.asyncPersistence.removeSubscriptions(client, subs)
    // promisify
    const mappedSubs = subs.map(sub => { return { topic: sub } })
    const remSubs2 = new Promise((resolve, reject) => {
      this._removedSubscriptions(client, mappedSubs, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
    Promise.all([remSubs1, remSubs2])
      .then(() => process.nextTick(cb, null, client))
      .catch(err => cb(err, client))
  }

  subscriptionsByClient (client, cb) {
    if (!this.ready) {
      this.once('ready', this.subscriptionsByClient.bind(this, client, cb))
      return
    }

    this.asyncPersistence.subscriptionsByClient(client)
      .then(results => process.nextTick(cb, null, results.length > 0 ? results : null, client))
      .catch(cb)
  }

  countOffline (cb) {
    this.asyncPersistence.countOffline()
      .then(res => process.nextTick(cb, null, res.subscriptionsCount, res.clientsCount))
      .catch(cb)
  }

  destroy (cb) {
    if (!this.ready) {
      this.once('ready', this.destroy.bind(this, cb))
      return
    }

    if (this._destroyed) {
      throw new Error('destroyed called twice!')
    }

    this._destroyed = true

    const finish = cb || noop

    this.asyncPersistence.destroy()
      .finally(finish) // swallow err in case of failure
  }

  outgoingEnqueue (sub, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingEnqueue.bind(this, sub, packet, cb))
      return
    }
    this.asyncPersistence.outgoingEnqueue(sub, packet)
      .then(() => process.nextTick(cb, null, packet))
      .catch(cb)
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingEnqueueCombi.bind(this, subs, packet, cb))
      return
    }
    this.asyncPersistence.outgoingEnqueueCombi(subs, packet)
      .then(() => process.nextTick(cb, null, packet))
      .catch(cb)
  }

  outgoingStream (client) {
    return Readable.from(this.asyncPersistence.outgoingStream(client))
  }

  outgoingUpdate (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingUpdate.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.outgoingUpdate(client, packet)
      .then(() => cb(null, client, packet))
      .catch(cb)
  }

  outgoingClearMessageId (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.outgoingClearMessageId.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.outgoingClearMessageId(client, packet)
      .then((packet) => cb(null, packet))
      .catch(cb)
  }

  incomingStorePacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingStorePacket.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.incomingStorePacket(client, packet)
      .then(() => cb(null))
      .catch(cb)
  }

  incomingGetPacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingGetPacket.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.incomingGetPacket(client, packet)
      .then((packet) => cb(null, packet, client))
      .catch(cb)
  }

  incomingDelPacket (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.incomingDelPacket.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.incomingDelPacket(client, packet)
      .then(() => cb(null))
      .catch(cb)
  }

  putWill (client, packet, cb) {
    if (!this.ready) {
      this.once('ready', this.putWill.bind(this, client, packet, cb))
      return
    }
    this.asyncPersistence.putWill(client, packet)
      .then(() => cb(null, client))
      .catch(cb)
  }

  getWill (client, cb) {
    this.asyncPersistence.getWill(client)
      .then((packet) => cb(null, packet, client))
      .catch(cb)
  }

  delWill (client, cb) {
    this.asyncPersistence.delWill(client)
      .then((packet) => cb(null, packet, client))
      .catch(cb)
  }

  streamWill (brokers) {
    return Readable.from(this.asyncPersistence.streamWill(brokers))
  }

  getClientList (topic) {
    return Readable.from(this.asyncPersistence.getClientList(topic))
  }
}

function noop () {}

module.exports = (opts) => new RedisPersistence(opts)
