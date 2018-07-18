// npm install mqtt fastq
// command to run : node fastbench 25000
'use strict'

var queue = require('fastq')(worker, 1)
var mqtt = require('mqtt')
var connected = 1
var clients = []
var count = 0
var subscriptions = 0
var st = Date.now()
console.log('Started connecting clients', st)

setInterval(function () {
  console.log(new Date() + '-' + count + ' - ' + subscriptions)
}, 1000)

function worker (arg, cb) {
  clients[count] = mqtt.connect({
    port: 1883,
    keepalive: 60
  })
  clients[count].on('connect',
    (function (clientObj) {
      return function () {
        connected++
        if (connected === process.argv[2]) {
          console.log('done connecting clients', Date.now() - st)
        }
        clientObj.subscribe(clientObj.options.clientId, function () {
          subscriptions++
        })
        cb(null, 42 * 2)
      }
    }(clients[count]))
  )
  count++
}

for (var i = 0; i < process.argv[2]; i++) {
  queue.push(42, function () {})
}
