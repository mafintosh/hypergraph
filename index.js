var hash = require('./lib/hash')
var messages = require('./lib/messages')
var ReadStream = require('./lib/read-stream')
var WriteStream = require('./lib/write-stream')
var DiffStream = require('./lib/diff-stream')
var MatchStream = require('./lib/match-stream')
var lexint = require('lexicographic-integer')
var thunky = require('thunky')
var varint = require('varint')
var ids = require('numeric-id-map')
var debug = require('debug')('dat-graph')

module.exports = DAG

function DAG (db) {
  if (!(this instanceof DAG)) return new DAG(db)

  this.db = db
  this.paths = []
  this.flushedPaths = []
  this.headPaths = []
  this.inserting = ids()

  this.ready = thunky(init.bind(null, this))
}

DAG.prototype.count = function (opts, cb) {
  if (typeof opts === 'function') return this.count(null, opts)
  if (!opts) opts = {}

  this._range(opts, function (err, since, until) {
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

DAG.prototype.close = function (cb) {
  this.db.close(cb)
}

DAG.prototype.createDiffStream = function () {
  return new DiffStream(this)
}

DAG.prototype.createMatchStream = function () {
  return new MatchStream(this)
}

DAG.prototype.match = function (hashes, cb) {
  var matches = []
  var missing = hashes.length
  var error = null

  for (var i = 0; i < hashes.length; i++) this.get(hashes[i], check(i))

  function check (i) {
    return function (err, node) {
      if (err && !err.notFound) error = err
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

    // if there are no heads
    if (!self.headPaths.length) return cb(null, [])

    var error = null
    var missing = 0
    var nodes = []

    for (var i = 0; i < self.headPaths.length; i++) {
      if (!self.headPaths[i]) continue
      push(i, self.headPaths[i])
    }

    function push (path, seq) {
      missing++
      getPathNode(self, path, seq, function (err, node) {
        if (err) error = err
        else nodes.push(node)
        if (--missing) return
        if (error) return cb(error)
        var heads = nodes.sort(compareNodes)
        debug('heads', heads)
        cb(null, heads)
      })
    }
  })
}

DAG.prototype.createReadStream = function (opts) {
  return new ReadStream(this, opts)
}

DAG.prototype.createWriteStream = function (opts) {
  return new WriteStream(this, opts)
}

DAG.prototype.get = function (key, cb) {
  debug('get', key)
  this.db.get('!nodes!' + key.toString('hex'), {valueEncoding: messages.Node}, cb)
}

// TODO we should check that value is a valid value and throw
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
    path: 0,
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
        if (err) { // on batch error reset our path
          if (node.seq) self.paths[node.path]--
          self.inserting.remove(id)
          return cb(err)
        }

        // update flushed caches
        for (var i = 0; i < links.length; i++) {
          var link = links[i]
          if (self.headPaths[link.path] === link.seq) self.headPaths[link.path] = 0
        }
        insert(self.headPaths, node.path, node.seq)
        insert(self.flushedPaths, node.path, node.seq)

        self.inserting.remove(id)
        cb(null, node)
      }
    })
  })
}

DAG.prototype._range = function (opts, cb) {
  if (typeof opts === 'function') return this._range(null, opts)
  if (!opts) opts = {}

  var self = this
  var since = opts.since || []
  var until = opts.until || []

  this.ready(function (err) {
    if (err) return cb(err)
    if (!since.length && !until.length) return cb(null, [], self.flushedPaths)

    var error = null
    var missing = since.length + until.length
    var untilPaths = []
    var sincePaths = []

    for (var i = 0; i < since.length; i++) self.get(toKey(since[i]), addSince)
    for (var j = 0; j < until.length; j++) self.get(toKey(until[j]), addUntil)

    function addSince (err, node) {
      if (err) return cb(err)
      addPaths(self, node, sincePaths, check)
    }

    function addUntil (err, node) {
      if (err) return cb(err)
      addPaths(self, node, untilPaths, check)
    }

    function check (err) {
      if (err) error = err
      if (--missing) return
      if (error) cb(error)
      else cb(null, sincePaths, untilPaths.length ? untilPaths : self.flushedPaths)
    }
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
    if (dag.flushedPaths[node.path] < node.seq) return cb(new Error('Linked node is not flushed'))
    node.sort = Math.max(node.sort, links[j].sort + 1)
  }

  if (links.length === 1 && dag.paths[links[0].path] === links[0].seq) {
    // fast track - the single previous linked path was a head
    node.path = links[0].path
    node.seq = ++dag.paths[node.path]
    return cb(null, null)
  }
  if (!links.length) {
    // no links - create a new path
    node.path = dag.paths.length
    node.seq = 1
    dag.paths[node.path] = node.seq
    return cb(null, null)
  }

  // slow(ish) track - do a path cache lookup
  updateNodeFromCache(dag, node, links, cb)
}

function updateNodeFromCache (dag, node, links, cb) {
  getPaths(dag, links, function (err, paths) {
    if (err) return cb(err)

    for (var i = 0; i < paths.length; i++) {
      if (dag.paths[i] === paths[i]) {
        // linked path is a head - use that one
        node.path = i
        node.seq = dag.paths[i] = paths[i] + 1
        return cb(null, paths)
      }
    }

    // no linked path is a head - create a new path
    node.path = dag.paths.length
    node.seq = 1
    dag.paths[node.path] = node.seq

    cb(null, paths)
  })
}

function createBatch (key, node, cache) {
  var batch = new Array(cache ? 3 : 2)
  var offset = 0

  if (cache) {
    batch[offset++] = {
      type: 'put',
      key: '!heads!' + node.path + '!' + pack(node.seq),
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
    key: '!paths!' + node.path + '!' + pack(node.seq),
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
      gt: '!paths!',
      lt: '!paths!' + prev + '!',
      valueAsBuffer: true,
      limit: 1,
      reverse: true
    }

    var ite = dag.db.db.iterator(opts)
    ite.next(function (err, key, val) {
      ite.end(noop)
      if (err) return cb(err)
      if (!key) return addHeads(dag, dag.headPaths, done)
      dag.get(val, function (err, node) {
        if (err) return cb(err)
        addPaths(dag, node, dag.paths, function (err) {
          if (err) return cb(err)
          run(node.path.toString())
        })
      })
    })
  }

  function done (err) {
    if (err) return cb(err)
    dag.flushedPaths = dag.paths.slice(0) // copy
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
    if (dag.headPaths[n.path] === n.seq) return true
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
  if (!dag.paths.length) return cb()

  var error = null
  var missing = dag.paths.length

  for (var i = 0; i < dag.paths.length; i++) heads[i] = 1
  for (var j = 0; j < dag.paths.length; j++) checkNode(j, dag.paths[j], next)

  function next (err) {
    if (err) error = err
    if (--missing) return
    for (var i = 0; i < heads.length; i++) {
      if (heads[i]) heads[i] = dag.paths[i]
    }
    cb(error)
  }

  function checkNode (path, seq, cb) {
    var paths = []
    getPathNode(dag, path, seq, function (err, node) {
      if (err) return cb(err)
      addPaths(dag, node, paths, function (err) {
        if (err) return cb(err)
        for (var i = 0; i < paths.length; i++) {
          if (paths[i] === dag.paths[i] && path !== i) heads[i] = 0
        }
        cb()
      })
    })
  }
}

function getPathNode (dag, path, seq, cb) {
  dag.db.get('!paths!' + path + '!' + pack(seq), {valueEncoding: 'binary'}, function (err, key) {
    if (err) return cb(err)
    dag.get(key, cb)
  })
}

function getPaths (dag, nodes, cb) {
  if (!nodes.length) return cb(null, [])

  var paths = []
  var missing = nodes.length
  var error = null

  for (var i = 0; i < nodes.length; i++) addPaths(dag, nodes[i], paths, next)

  function next (err) {
    if (err) error = err
    if (--missing) return
    if (error) cb(error)
    else cb(null, paths)
  }
}

function addPaths (dag, node, paths, cb) {
  var prefix = '!heads!' + node.path + '!'
  var opts = {
    gt: prefix,
    lte: prefix + pack(node.seq),
    valueAsBuffer: true,
    limit: 1,
    reverse: true
  }

  insert(paths, node.path, node.seq)

  var ite = dag.db.db.iterator(opts)
  ite.next(function (err, key, val) {
    ite.end(noop)

    if (err) return cb(err)
    if (!val) return cb(null)

    var prev = unpackArray(val)
    for (var i = 0; i < prev.length; i++) insert(paths, i, prev[i])

    cb(null)
  })
}

function insert (paths, id, seq) {
  for (var i = paths.length; i <= id; i++) paths[i] = 0 // TODO needs comment
  paths[id] = Math.max(seq, paths[id])
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
