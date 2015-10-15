var ids = require('numeric-id-map')
var lexint = require('lexicographic-integer')
var stream = require('readable-stream')
var util = require('util')
var messages = require('./messages')

var QUESTION_BATCH = 1 // max two messages

module.exports = BisectStream // TODO rename to e.g. DiffStream

function BisectStream (dag, opts) {
  if (!opts) opts = {}
  if (!(this instanceof BisectStream)) return new BisectStream(dag, opts)
  stream.Duplex.call(this)

  var self = this

  this.destroyed = false
  this.roundtrips = 0

  this._dag = dag
  this._questions = ids()
  this._logs = 0

  dag.range(function (err, since, until) {
    if (err) return self.destroy(err)
    self._logs = until.length
    for (var i = 0; i < until.length; i++) self._divide(i, i < since.length ? since[i] : 0, until[i])
  })
}

util.inherits(BisectStream, stream.Duplex)

BisectStream.prototype._read = noop

BisectStream.prototype._flush = function () {
  if (this._logs === this._questions.length) this.emit('flush')
}

BisectStream.prototype._divide = function (log, since, until) {
  var self = this
  var delta = Math.floor((until - since) / (1 + QUESTION_BATCH))
  var keys = []
  var indexes = []

  loop(since + 1)

  function send () {
    self.roundtrips++

    var data = messages.Question.encode({
      id: self._questions.add(done),
      keys: keys
    })

    self.push(data)
  }

  function done () {
    console.log('??')
  }

  function loop (last) {
    var next = last + delta
    if (keys.length === QUESTION_BATCH || next > until) return send()
    getLogKey(self._dag, log, last + delta, function (err, key) {
      if (err) return self.destroy(err)
      keys.push(key)
      loop(delta === 0 ? next + 1 : next)
    })
  }
}

BisectStream.prototype._write = function (data, enc, cb) {
  console.log('write', data)
  cb()
}

BisectStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}

function noop () {}

function getLogKey (dag, log, seq, cb) {
  dag.db.get('!logs!' + log + '!' + lexint.pack(seq, 'hex'), {valueEncoding: 'binary'}, cb)
}

