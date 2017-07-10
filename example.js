var mq = require('mqemitter-redis')()
var persistence = require('.')()
var aedes = require('aedes')({
  mq,
  persistence
})
var server = require('net').createServer(aedes.handle)

server.listen(1883)
