// npm install mqtt fastq
// command to run : node fastbench 25000
const queue = require('fastq')(worker, 1)
const mqtt = require('mqtt')
let connected = 1
const clients = []
let count = 0
let subscriptions = 0
const st = Date.now()
console.log('Started connecting clients', st)

setInterval(() => {
  console.log(`${new Date()}-${count} - ${subscriptions}`)
}, 1000)

function worker (arg, cb) {
  clients[count] = mqtt.connect({
    port: 1883,
    keepalive: 60
  })
  clients[count].on('connect', (clientObj => {
    return () => {
      connected++
      if (connected === process.argv[2]) {
        console.log('done connecting clients', Date.now() - st)
      }
      clientObj.subscribe(clientObj.options.clientId, () => {
        subscriptions++
      })
      cb(null, 42 * 2)
    }
  })(clients[count]))
  count++
}

for (let i = 0; i < process.argv[2]; i++) {
  queue.push(42, () => {})
}
