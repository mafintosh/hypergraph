var stream = require('readable-stream')
var util = require('util')

var QUESTION_BATCH = 1 // max two messages

module.exports = BisectStream

function BisectStream (dag) {
  if (!(this instanceof BisectStream)) return new BisectStream(dag)
  stream.Duplex.call(this)
  var self = this

  this._dag = dag
  this.destroyed = false
  this.roundtrips = 0

  dag.range(function (err, since, until) {
    if (err) return self.destroy(err)
    for (var i = 0; i < until.length; i++) bisect(i, i < since.length ? since[i] : 0, until[i])
  })

  function bisect (log, since, until) {
    var delta = Math.floor((until - since) / (1 + QUESTION_BATCH))
    var keys = []

    console.log('bisect', log, since, until, delta)
    loop(since)

    function loop (last) {

    }
  }
}

util.inherits(BisectStream, stream.Duplex)

BisectStream.prototype._read = noop

BisectStream.prototype._write = function (data, enc, cb) {
  console.log(data)
  cb()
}

BisectStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}

function noop () {}