var ids = require('numeric-id-map')
var lexint = require('lexicographic-integer')
var stream = require('readable-stream')
var util = require('util')
var messages = require('./messages')

var QUESTION_BATCH = 32

module.exports = DiffStream

function DiffStream (dag, opts) {
  if (!opts) opts = {}
  if (!(this instanceof DiffStream)) return new DiffStream(dag, opts)
  stream.Duplex.call(this, {objectMode: true, highWaterMark: 16})

  var self = this

  this.destroyed = false
  this.roundtrips = 0
  this.messages = 0
  this.since = []
  this.binary = !!opts.binary

  this._seqs = []
  this._heads = []
  this._dag = dag
  this._questions = ids()
  this._paths = 0
  this._finished = 0

  dag._range(opts, function (err, since, until) {
    if (err) return self.destroy(err)
    if (!until.length) return self._end()
    self._paths = until.length
    for (var i = 0; i < until.length; i++) {
      self._heads[i] = i < since.length ? since[i] : 0
      self._divide(i, self._heads[i], until[i])
    }
  })
}

util.inherits(DiffStream, stream.Duplex)

DiffStream.prototype._read = noop

DiffStream.prototype._flush = function () {
  if (this._paths === this._questions.length) this.emit('flush')
}

DiffStream.prototype._divide = function (path, since, until) {
  if (since < this._heads[path]) since = this._heads[path]

  var self = this
  var delta = Math.floor((until - since) / (1 + QUESTION_BATCH))
  var keys = []

  if (since >= until) return done(-1)
  loop(since + 1)

  function send () {
    self.messages++
    if (!keys.length) return done(-1)

    var q = {
      id: self._questions.add(done),
      keys: keys
    }

    self.push(self.binary ? messages.Question.encode(q) : q)
    self._flush()
  }

  function done (max) {
    if (delta <= 0) return self._finish(path, since + 1 + max, max === -1 ? null : keys[max])

    // smaller than the first chunk
    if (max === -1) return self._divide(path, since, since + delta)

    var maxIndex = since + 1 + delta * (max + 1)

    // larger than the last chunk
    if (max === keys.length - 1) return self._divide(path, maxIndex - 1, until)

    // inbetween two chunks
    self._divide(path, maxIndex - 1, maxIndex + delta)
  }

  function loop (last) {
    var next = last + delta
    if (keys.length === QUESTION_BATCH || next > until) return send()
    getPathKey(self._dag, path, last + delta, function (err, key) {
      if (err) return self.destroy(err)
      keys.push(key)
      loop(delta === 0 ? next + 1 : next)
    })
  }
}

DiffStream.prototype._finish = function (path, seq, key) {
  var self = this

  if (!key || this._heads[path] >= seq) return done()

  this._dag._heads(key, function (err, paths) {
    if (err) return self.destroy(err)

    for (var i = 0; i < paths.length; i++) {
      if (i !== path && self._heads[i] < paths[i]) self._heads[i] = paths[i]
    }

    insert(self.since, path, null, key)
    insert(self._seqs, path, 0, seq)
    done()
  })

  function done () {
    self._finished++
    if (self._finished === self._paths) {
      self.since = self.since.filter(dedupe)
      self._end()
    }
  }

  function dedupe (key, path) {
    return self._seqs[path] > self._heads[path]
  }
}

DiffStream.prototype._end = function () {
  this.push(null)
}

DiffStream.prototype._write = function (data, enc, cb) {
  var a = this.binary ? messages.Answer.decode(data) : data

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

function insert (arr, index, def, val) {
  while (arr.length <= index) arr.push(def)
  arr[index] = val
}

function getPathKey (dag, path, seq, cb) {
  dag.db.get('!paths!' + path + '!' + lexint.pack(seq, 'hex'), {valueEncoding: 'binary'}, cb)
}
