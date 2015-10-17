var stream = require('readable-stream')
var util = require('util')
var messages = require('./messages')

module.exports = WriteStream

function WriteStream (dag, opts) {
  if (!(this instanceof WriteStream)) return new WriteStream(dag, opts)
  if (!opts) opts = {}
  stream.Writable.call(this, {objectMode: true, highWaterMark: 16})
  this.binary = !!opts.binary
  this.destroyed = false
  this._dag = dag
}

util.inherits(WriteStream, stream.Writable)

WriteStream.prototype._write = function (data, enc, cb) {
  if (this.destroyed) return cb()
  var node = this.binary ? messages.WireNode.decode(data) : data
  this._dag.add(node.links, node.value, cb)
}

WriteStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}
