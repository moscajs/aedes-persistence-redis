const mq = require('mqemitter-redis')()
const persistence = require('.')()
const aedes = require('aedes')({
  mq,
  persistence
})
const server = require('node:net').createServer(aedes.handle)

server.listen(1883)
