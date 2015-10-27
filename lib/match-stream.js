var stream = require('readable-stream')
var util = require('util')
var messages = require('./messages')

module.exports = MatchStream

function MatchStream (dag, opts) {
  if (!opts) opts = {}
  if (!(this instanceof MatchStream)) return new MatchStream(dag, opts)
  stream.Duplex.call(this, {objectMode: true, highWaterMark: 16})

  this.destroyed = false
  this.binary = !!opts.binary

  this._dag = dag
  this._pending = 0

  this.on('finish', this._kick)
}

util.inherits(MatchStream, stream.Duplex)

MatchStream.prototype._read = noop

MatchStream.prototype._write = function (data, enc, cb) {
  var self = this
  var q = self.binary ? messages.Question.decode(data) : data

  this._pending++
  this._dag.match(q.keys, function (err, matches) {
    self._pending--
    if (err) return self.destroy(err)
    var a = {id: q.id, matches: matches}
    self.push(self.binary ? messages.Answer.encode(a) : a)
    self._kick()
  })

  cb()
}

MatchStream.prototype._kick = function () {
  if (this._writableState.finished && !this._pending) this.push(null)
}

MatchStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}

function noop () {}
