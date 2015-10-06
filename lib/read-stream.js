var lexint = require('lexicographic-integer')
var pump = require('pump')
var through = require('through2')
var stream = require('readable-stream')
var util = require('util')

module.exports = ReadStream

function ReadStream (dag, opts) {
  if (!(this instanceof ReadStream)) return new ReadStream(dag, opts)
  stream.Readable.call(this, {objectMode: true, highWaterMark: 16})

  this.destroyed = false
  this._want = true
  this._streams = null
  this._dag = dag

  var self = this

  dag.ready(function (err) {
    if (err) return self.destroy(err)
    if (self.destroyed) return
    self._streams = dag.flushedPaths.map(onpath)
    self._streams.forEach(onstream)
  })

  function onpath (seq, path) {
    var opts = {
      gt: '!paths!' + path + '!',
      lte: '!paths!' + path + '!' + lexint.pack(seq, 'hex'),
      valueEncoding: 'binary'
    }

    return pump(dag.db.createValueStream(opts), through.obj(get))

    function get (key, enc, cb) {
      dag.get(key, cb)
    }
  }

  function onstream (s) {
    s.on('error', function (err) {
      self.destroy(err)
    })

    s.on('readable', function () {
      if (!self._want) return
      self._want = false
      self._read()
    })
  }
}

util.inherits(ReadStream, stream.Readable)

ReadStream.prototype._read = function () {
  if (this._streams) this._update()
}

ReadStream.prototype._end = function () {
  this._streams.forEach(function (s) {
    s.read()
  })
  this.push(null)
}

ReadStream.prototype._update = function () {
  var updated = false

  while (true) {
    var min = null
    for (var i = 0; i < this._streams.length; i++) {
      var state = this._streams[i]._readableState
      var b = state.buffer
      if (state.ended && !b.length) continue
      if (!b.length) {
        if (!updated) this._want = true
        return
      }
      if (!min || min.sort > b[0].sort) min = b[0]
    }
    if (!min) return this._end()
    var next = this._streams[min.path].read()
    updated = true
    if (!this.push(next)) {
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
