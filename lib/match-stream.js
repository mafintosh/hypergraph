var stream = require('readable-stream')
var util = require('util')
var messages = require('./messages')

module.exports = MatchStream

function MatchStream (dag) {
  if (!(this instanceof MatchStream)) return new MatchStream(dag)
  stream.Duplex.call(this)
  this.destroyed = false
  this._dag = dag
}

util.inherits(MatchStream, stream.Duplex)

MatchStream.prototype._read = noop

MatchStream.prototype._write = function (data, enc, cb) {
  var self = this
  var q = messages.Question.decode(data)
// console.log('matching', data.length, q)
  this._dag.match(q.keys, function (err, matches) {
    if (err) return self.destroy(err)
    self.push(messages.Answer.encode({id: q.id, matches: matches}))
  })

  cb()
}

MatchStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}

function noop () {}
