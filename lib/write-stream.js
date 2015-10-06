var stream = require('readable-stream')
var util = require('util')

module.exports = WriteStream

function WriteStream (dag) {
  if (!(this instanceof WriteStream)) return new WriteStream(dag)
  stream.Writable.call(this, {objectMode: true, highWaterMark: 16})
  this.destroyed = false
  this._dag = dag
}

util.inherits(WriteStream, stream.Writable)

WriteStream.prototype._write = function (data, enc, cb) {
  if (this.destroyed) return cb()
  this._dag.add(data.links, data.value, cb)
}

WriteStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}
