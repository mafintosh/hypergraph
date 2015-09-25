var hyperlog = require('hyperlog')
var duplexify = require('duplexify')
var subleveldown = require('subleveldown')
var bulk = require('bulk-write-stream')
var lexint = require('lexicographic-integer')
var crypto = require('crypto')

module.exports = Graph

function Graph (db) {
  this.log = hyperlog(subleveldown(db, 'log'))
  this.tags = subleveldown(db, 'tags')
  this.data = subleveldown(db, 'data')
}

Graph.prototype.get = function (key) {
  key = typeof key === 'string' ? key : key.key

  var self = this
  var proxy = duplexify()

  this.log.get(key, function (err, node) {
    if (proxy.destroyed) return
    if (err) return proxy.destroy(err)

    var data = node.value.toString('hex')
    self.tags.get(data, function (_, ref) {
      if (!ref) ref = data
      if (proxy.destroyed) return

      proxy.setReadable(self.data.createValueStream({gt: '!' + ref + '!', lt: '!' + ref + '!~'}))
      proxy.setWritable(false)
    })
  })

  return proxy
}

Graph.prototype.add = function (links) {
  var self = this
  var tmp = crypto.randomBytes(32).toString('hex')
  var hash = crypto.createHash('sha256')
  var index = 0

  var stream = bulk(write, flush)
  stream.node = null
  return stream

  function write (batch, cb) {
    var b = new Array(batch.length)

    for (var i = 0; i < batch.length; i++) {
      hash.update(batch[i])
      b[i] = {
        type: 'put',
        key: '!' + tmp + '!' + lexint.pack(index++, 'hex'),
        value: batch[i],
        valueEncoding: 'binary'
      }
    }

    self.data.batch(b, cb)
  }

  function flush (cb) {
    var key = hash.digest('hex')
    self.tags.put(key, tmp, function (err) {
      if (err) return cb(err)
      self.log.add(links, new Buffer(key, 'hex'), function (err, node) {
        if (err) return cb(err)
        stream.node = node
        cb()
      })
    })
  }
}

var memdb = require('memdb')
var graph = new Graph(memdb())

var stream = graph.add()

stream.write(new Buffer('hello world #1'))
stream.write(new Buffer('hello world #2'))
stream.write(new Buffer('hello world #3'))

stream.end(function () {
  graph.get(stream.node).on('data', console.log)
})