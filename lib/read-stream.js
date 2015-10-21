var lexint = require('lexicographic-integer')
var pump = require('pump')
var through = require('through2')
var stream = require('readable-stream')
var util = require('util')
var messages = require('./messages')

module.exports = ReadStream

function ReadStream (dag, opts) {
  if (!(this instanceof ReadStream)) return new ReadStream(dag, opts)
  if (!opts) opts = {}
  stream.Readable.call(this, {objectMode: true, highWaterMark: 16})

  this.binary = !!opts.binary
  this.destroyed = false
  this.length = 0

  this._want = true
  this._streams = null
  this._dag = dag

  var self = this

  dag._range(opts, function (err, since, until) {
    if (err) return self.destroy(err)
    if (self.destroyed) return

    self._streams = until.map(toStream).filter(valid)
    process.nextTick(setup)

    function setup () {
      self.emit('ready')
    }

    function toStream (untilSeq, path) {
      var sinceSeq = since.length > path ? since[path] : 0
      if (untilSeq <= sinceSeq) return null

      self.length += untilSeq - sinceSeq

      var opts = {
        gt: '!paths!' + path + '!' + lexint.pack(sinceSeq, 'hex'),
        lte: '!paths!' + path + '!' + lexint.pack(untilSeq, 'hex'),
        valueEncoding: 'binary'
      }

      var s = pump(dag.db.createValueStream(opts), through.obj(get))
      s.on('error', onerror)
      s.on('readable', onreadable)
      return s
    }
  })

  function get (key, _, cb) {
    dag.get(key, cb)
  }

  function onreadable (s) {
    if (!self._want) return
    self._want = false
    self._read()
  }

  function onerror (err) {
    self.destroy(err)
  }
}

util.inherits(ReadStream, stream.Readable)

ReadStream.prototype._read = function () {
  if (this._streams) this._update()
}

ReadStream.prototype._end = function () {
  this._streams.forEach(function (s) {
    s.read() // will end the streams
  })
  this.push(null)
}

ReadStream.prototype._update = function () {
  var updated = false

  while (true) {
    var minIndex = 0
    var min = null
    for (var i = 0; i < this._streams.length; i++) {
      var state = this._streams[i]._readableState
      var b = state.buffer
      if (state.ended && !b.length) continue
      if (!b.length) {
        if (!updated) this._want = true
        return
      }
      if (!min || min.sort > b[0].sort) {
        minIndex = i
        min = b[0]
      }
    }
    if (!min) return this._end()
    var next = this._streams[minIndex].read()
    updated = true
    if (!this.push(this.binary ? messages.WireNode.encode(next) : next)) {
      if (!updated) this._want = true
      return
    }
  }
}

ReadStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  if (err) this.emit('error', err)
  if (this._streams) {
    this._streams.forEach(function (s) {
      s.destroy()
    })
  }
  this.emit('close')
}

function valid (val) {
  return val
}
