var ids = require('numeric-id-map')
var lexint = require('lexicographic-integer')
var stream = require('readable-stream')
var util = require('util')
var messages = require('./messages')

var QUESTION_BATCH = 16 // max two messages

module.exports = DiffStream

function DiffStream (dag, opts) {
  if (!opts) opts = {}
  if (!(this instanceof DiffStream)) return new DiffStream(dag, opts)
  stream.Duplex.call(this)

  var self = this

  this.destroyed = false
  this.roundtrips = 0
  this.messages = 0
  this.since = []

  this._dag = dag
  this._questions = ids()
  this._logs = 0
  this._finished = 0

  dag.range(function (err, since, until) {
    if (err) return self.destroy(err)
    self._logs = until.length
    for (var i = 0; i < until.length; i++) self._divide(i, i < since.length ? since[i] : 0, until[i])
  })
}

util.inherits(DiffStream, stream.Duplex)

DiffStream.prototype._read = noop

DiffStream.prototype._flush = function () {
  if (this._logs === this._questions.length) this.emit('flush')
}

DiffStream.prototype._divide = function (log, since, until) {
  var self = this
  var delta = Math.floor((until - since) / (1 + QUESTION_BATCH))
  var keys = []
  var indexes = []

  if (since >= until) return done(-1)
  loop(since + 1)

  function send () {
    self.messages++
    if (!keys.length) return done(-1)

    var data = messages.Question.encode({
      id: self._questions.add(done),
      keys: keys
    })

    self.push(data)
  }

  function done (max) {
    if (delta <= 0) {
      self._finished++
      if (max > -1) self.since.push(keys[max])
      if (self._finished === self._logs) self.push(null)
      return
    }

    if (max === -1) return self._divide(log, since, since + delta)

    var index = since + 1 + delta * (max + 1)
    self._divide(log, index, until)
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

DiffStream.prototype._write = function (data, enc, cb) {
  var a = messages.Answer.decode(data)

  var max = -1
  for (var i = 0; i < a.matches.length; i++) max = Math.max(max, a.matches[i])

  var done = this._questions.remove(a.id)
  if (done) done(max)

  cb()
}

DiffStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}

function noop () {}

function getLogKey (dag, log, seq, cb) {
  dag.db.get('!logs!' + log + '!' + lexint.pack(seq, 'hex'), {valueEncoding: 'binary'}, cb)
}

