var hash = require('./lib/hash')
var messages = require('./lib/messages')
var ReadStream = require('./lib/read-stream')
var WriteStream = require('./lib/write-stream')
var BisectStream = require('./lib/bisect-stream')
var lexint = require('lexicographic-integer')
var thunky = require('thunky')
var varint = require('varint')
var ids = require('numeric-id-map')

module.exports = DAG

function DAG (db) {
  if (!(this instanceof DAG)) return new DAG(db)

  this.db = db
  this.logs = []
  this.flushedLogs = []
  this.headLogs = []
  this.inserting = ids()

  this.ready = thunky(init.bind(null, this))
}

DAG.prototype.count = function (opts, cb) {
  if (typeof opts === 'function') return this.count(null, opts)
  if (!opts) opts = {}

  this.range(opts, function (err, since, until) {
    if (err) return cb(err)
    var sum = 0
    for (var i = 0; i < until.length; i++) {
      if (i >= since.length) sum += until[i]
      else if (until[i] > since[i]) sum += until[i] - since[i]
    }
    process.nextTick(function () {
      cb(null, sum)
    })
  })
}

// TODO: this name seems leaky - find a better one?
DAG.prototype.createBisectStream = function () {
  return new BisectStream(this)
}

DAG.prototype.match = function (hashes, cb) {
  var matches = []
  var missing = hashes.length
  var error = null

  for (var i = 0; i < hashes.length; i++) this.get(hashes[i], check(i))

  function check (i) {
    return function (err, node) {
      if (err) error = err
      if (node) matches.push(i)
      if (--missing) return
      if (error) cb(error)
      else cb(null, matches)
    }
  }
}

DAG.prototype.heads = function (cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    var error = null
    var missing = 0
    var nodes = []

    for (var i = 0; i < self.headLogs.length; i++) {
      if (!self.headLogs[i]) continue
      push(i, self.headLogs[i])
    }

    function push (log, seq) {
      missing++
      getLogNode(self, log, seq, function (err, node) {
        if (err) error = err
        else nodes.push(node)
        if (--missing) return
        if (error) return cb(error)
        cb(null, nodes.sort(compareNodes))
      })
    }
  })
}

DAG.prototype.createReadStream = function (opts) {
  return new ReadStream(this, opts)
}

DAG.prototype.createWriteStream = function () {
  return new WriteStream(this)
}

DAG.prototype.range = function (opts, cb) {
  if (typeof opts === 'function') return this.range(null, opts)
  if (!opts) opts = {}

  var self = this
  var since = opts.since || []
  var until = opts.until || []

  this.ready(function (err) {
    if (err) return cb(err)
    if (!since.length && !until.length) return cb(null, [], self.flushedLogs)

    var error = null
    var missing = since.length + until.length
    var untilLogs = []
    var sinceLogs = []

    for (var i = 0; i < since.length; i++) self.get(toKey(since[i]), addSince)
    for (var j = 0; j < until.length; j++) self.get(toKey(until[j]), addUntil)

    function addSince (err, node) {
      if (err) return cb(err)
      addLogs(self, node, sinceLogs, check)
    }

    function addUntil (err, node) {
      if (err) return cb(err)
      addLogs(self, node, untilLogs, check)
    }

    function check (err) {
      if (err) error = err
      if (--missing) return
      if (error) cb(error)
      else cb(null, sinceLogs, untilLogs.length ? untilLogs : self.flushedLogs)
    }
  })
}

DAG.prototype.get = function (key, cb) {
  this.db.get('!nodes!' + key.toString('hex'), {valueEncoding: messages.Node}, cb)
}

DAG.prototype.append = function (value, cb) {
  var self = this
  this.heads(function (err, links) {
    if (err) return cb(err)
    self.add(links, value, cb)
  })
}

DAG.prototype.add = function (links, value, cb) {
  if (!links) links = []
  if (!cb) cb = noop

  var self = this
  var node = {
    key: null,
    links: links.map(toKey),
    value: value,
    sort: 0,
    log: 0,
    seq: 0
  }

  node.key = hash(node)

  var key = node.key.toString('hex')
  var id = 0

  this.ready(function ready (err) {
    if (err) return cb(err)

    getNodes(self, node.links, function (err, links) {
      if (err) return cb(err)

      // fast track - if a link was a head we know this is a new node
      if (containsHead(self, links)) insertNode(null, null)
      else self.get(node.key, insertNode)

      function insertNode (_, prev) {
        if (prev) return cb(null, prev)

        // the following if detects if we insert the EXCACT same node twice before flushing
        // and retries in 100ms if that happens (probably not very likely).
        // a simple hash map seemed to consume a lot of cpu hence the array based solution
        if (self.inserting.list.indexOf(key) > -1) return setTimeout(ready, 100)
        id = self.inserting.add(key)

        updateNode(self, node, links, function (err, cache) {
          if (err) return done(err)
          self.db.batch(createBatch(key, node, cache), done)
        })
      }

      function done (err) {
        if (err) { // on batch error reset our log
          if (node.seq) self.logs[node.log]--
          self.inserting.remove(id)
          return cb(err)
        }

        // update flushed caches
        for (var i = 0; i < links.length; i++) {
          var link = links[i]
          if (self.headLogs[link.log] === link.seq) self.headLogs[link.log] = 0
        }
        insert(self.headLogs, node.log, node.seq)
        insert(self.flushedLogs, node.log, node.seq)

        self.inserting.remove(id)
        cb(null, node)
      }
    })
  })
}

function compareNodes (a, b) {
  var len = Math.min(a.key.length, b.key.length) // future proofing
  for (var i = 0; i < len; i++) {
    if (a.key[i] === b.key[i]) continue
    return a.key[i] - b.key[i]
  }
  return a.key.length - b.key.length
}

function updateNode (dag, node, links, cb) {
  for (var j = 0; j < links.length; j++) {
    // its very unlikely that this if will fire but it's here to prevent the head cache being messed up
    if (dag.flushedLogs[node.log] < node.seq) return cb(new Error('Linked node is not flushed'))
    node.sort = Math.max(node.sort, links[j].sort + 1)
  }

  if (links.length === 1 && dag.logs[links[0].log] === links[0].seq) {
    // fast track - the single previous linked log was a head
    node.log = links[0].log
    node.seq = ++dag.logs[node.log]
    return cb(null, null)
  }
  if (!links.length) {
    // no links - create a new log
    node.log = dag.logs.length
    node.seq = 1
    dag.logs[node.log] = node.seq
    return cb(null, null)
  }

  // slow(ish) track - do a log cache lookup
  updateNodeFromCache(dag, node, links, cb)
}

function updateNodeFromCache (dag, node, links, cb) {
  getLogs(dag, links, function (err, logs) {
    if (err) return cb(err)

    for (var i = 0; i < logs.length; i++) {
      if (dag.logs[i] === logs[i]) {
        // linked log is a head - use that one
        node.log = i
        node.seq = dag.logs[i] = logs[i] + 1
        return cb(null, logs)
      }
    }

    // no linked log is a head - create a new log
    node.log = dag.logs.length
    node.seq = 1
    dag.logs[node.log] = node.seq

    cb(null, logs)
  })
}

function createBatch (key, node, cache) {
  var batch = new Array(cache ? 3 : 2)
  var offset = 0

  if (cache) {
    batch[offset++] = {
      type: 'put',
      key: '!logcache!' + node.log + '!' + pack(node.seq),
      value: packArray(cache)
    }
  }

  batch[offset++] = {
    type: 'put',
    key: '!nodes!' + key,
    value: messages.Node.encode(node)
  }

  batch[offset++] = {
    type: 'put',
    key: '!logs!' + node.log + '!' + pack(node.seq),
    value: node.key
  }

  return batch
}

function init (dag, cb) {
  if (dag.db.isOpen()) run()
  else dag.db.on('open', run)

  function run (prev) {
    if (!prev) prev = '~'

    var opts = {
      gt: '!logs!',
      lt: '!logs!' + prev + '!',
      valueAsBuffer: true,
      limit: 1,
      reverse: true
    }

    var ite = dag.db.db.iterator(opts)
    ite.next(function (err, key, val) {
      ite.end(noop)
      if (err) return cb(err)
      if (!key) return addHeads(dag, dag.headLogs, done)
      dag.get(val, function (err, node) {
        if (err) return cb(err)
        addLogs(dag, node, dag.logs, function (err) {
          if (err) return cb(err)
          run(node.log.toString())
        })
      })
    })
  }

  function done (err) {
    if (err) return cb(err)
    dag.flushedLogs = dag.logs.slice(0) // copy
    cb()
  }
}

function noop () {}

function toKey (node) {
  if (Buffer.isBuffer(node)) return node
  if (typeof node === 'string') return new Buffer(node, 'hex')
  return node.key
}

function containsHead (dag, nodes) {
  for (var i = 0; i < nodes.length ; i++) {
    var n = nodes[i]
    if (dag.headLogs[n.log] === n.seq) return true
  }
  return false
}

function toKey (key) {
  if (Buffer.isBuffer(key)) return key
  if (typeof key === 'string') return new Buffer(key, 'hex')
  return key.key
}

function getNodes (dag, links, cb) {
  if (!links.length) return cb(null, [])

  var nodes = new Array(links.length)
  var error = null
  var missing = nodes.length

  for (var i = 0; i < links.length; i++) dag.get(links[i], add)

  function add (err, node) {
    if (err) error = err
    nodes[--missing] = node
    if (missing) return
    if (error) cb(error)
    else cb(null, nodes)
  }
}

function addHeads (dag, heads, cb) {
  if (!dag.logs.length) return cb()

  var error = null
  var missing = dag.logs.length

  for (var i = 0; i < dag.logs.length; i++) heads[i] = 1
  for (var j = 0; j < dag.logs.length; j++) checkNode(j, dag.logs[j], next)

  function next (err) {
    if (err) error = err
    if (--missing) return
    for (var i = 0; i < heads.length; i++) {
      if (heads[i]) heads[i] = dag.logs[i]
    }
    cb(error)
  }

  function checkNode (log, seq, cb) {
    var logs = []
    getLogNode(dag, log, seq, function (err, node) {
      if (err) return cb(err)
      addLogs(dag, node, logs, function (err) {
        if (err) return cb(err)
        for (var i = 0; i < logs.length; i++) {
          if (logs[i] === dag.logs[i] && log !== i) heads[i] = 0
        }
        cb()
      })
    })
  }
}

function getLogNode (dag, log, seq, cb) {
  dag.db.get('!logs!' + log + '!' + pack(seq), {valueEncoding: 'binary'}, function (err, key) {
    if (err) return cb(err)
    dag.get(key, cb)
  })
}

function getLogs (dag, nodes, cb) {
  if (!nodes.length) return cb(null, [])

  var logs = []
  var missing = nodes.length
  var error = null

  for (var i = 0; i < nodes.length; i++) addLogs(dag, nodes[i], logs, next)

  function next (err) {
    if (err) error = err
    if (--missing) return
    if (error) cb(error)
    else cb(null, logs)
  }
}

function addLogs (dag, node, logs, cb) {
  var prefix = '!logcache!' + node.log + '!'
  var opts = {
    gt: prefix,
    lte: prefix + pack(node.seq),
    valueAsBuffer: true,
    limit: 1,
    reverse: true
  }

  insert(logs, node.log, node.seq)

  var ite = dag.db.db.iterator(opts)
  ite.next(function (err, key, val) {
    ite.end(noop)

    if (err) return cb(err)
    if (!val) return cb(null)

    var prev = unpackArray(val)
    for (var i = 0; i < prev.length; i++) insert(logs, i, prev[i])

    cb(null)
  })
}

function insert (logs, id, seq) {
  for (var i = logs.length; i <= id; i++) logs[i] = 0 // TODO needs comment
  logs[id] = Math.max(seq, logs[id])
}

function unpackArray (buf) {
  var offset = 0
  var arr = []
  while (offset < buf.length) {
    arr.push(varint.decode(buf, offset))
    offset += varint.decode.bytes
  }
  return arr
}

function packArray (arr) {
  var size = 0
  var offset = 0
  for (var i = 0; i < arr.length; i++) {
    size += varint.encodingLength(arr[i])
  }
  var buf = new Buffer(size)
  for (var j = 0; j < arr.length; j++) {
    varint.encode(arr[j], buf, offset)
    offset += varint.encode.bytes
  }
  return buf
}

function pack (i) {
  return lexint.pack(i, 'hex')
}
