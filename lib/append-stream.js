var stream = require('readable-stream')
var util = require('util')

module.exports = AppendStream

function AppendStream (dag, opts) {
  if (!(this instanceof AppendStream)) return new AppendStream(dag, opts)
  if (!opts) opts = {}
  stream.Writable.call(this, {objectMode: true, highWaterMark: 16})
  this.destroyed = false
  this._dag = dag
  this._heads = opts.heads || null
}

util.inherits(AppendStream, stream.Writable)

AppendStream.prototype._write = function (data, enc, cb) {
  if (this.destroyed) return cb()
  if (!this._heads) return this._init(data, enc, cb)
  var self = this
  this._dag.add(this._heads, data, function (err, node) {
    if (err) return cb(err)
    self._heads = [node.key]
    cb()
  })
}

AppendStream.prototype._init = function (data, enc, cb) {
  var self = this
  this._dag.heads(function (err, heads) {
    if (err) return self.destroy(err)
    self._heads = heads
    self._write(data, enc, cb)
  })
}

AppendStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}
