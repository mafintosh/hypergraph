var DAG = require('./')
var test = require('tape')
var memdb = require('memdb')

test('.append and .get', function (t) {
  var graph = newDag()
  var testBuf = new Buffer('zuckerberg')
  graph.append(testBuf, function (err, node1) {
    if (err) return t.ifError(err, 'should not have err')
    graph.get(node1.key, function (err, node2) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(node1.key.toString(), node2.key.toString(), 'keys match')
      graph.close(t.end)
    })
  })
})

test('.add and .get', function (t) {
  var graph = newDag()
  var testBuf = new Buffer('ballmer')
  graph.add(null, testBuf, function (err, node1) {
    if (err) return t.ifError(err, 'should not have err')
    graph.get(node1.key, function (err, node2) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(node1.key.toString(), node2.key.toString(), 'keys match')
      graph.close(t.end)
    })
  })
})

test('.add and .heads', function (t) {
  var graph = newDag()
  var testBuf = new Buffer('dorsey')
  graph.add(null, testBuf, function (err, node1) {
    if (err) return t.ifError(err, 'should not have err')
    graph.heads(function (err, heads) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(heads.length, 1, 'has 1 head')
      t.equal(node1.key.toString('hex'), heads[0].key.toString('hex'), 'key is head')
      graph.close(t.end)
    })
  })
})

test('.createWriteStream and .count', function (t) {
  var graph = newDag()
  writeSomeBros(graph, function () {
    graph.count(function (err, count) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(count, 3, '3 bros')
      graph.close(t.end)
    })
  })
})

test('.createWriteStream and .createReadStream', function (t) {
  var graph = newDag()
  writeSomeBros(graph, function () {
    var count = 0
    var rs = graph.createReadStream()
    rs.on('data', function (node) {
      count += 1
    })
    rs.on('end', function () {
      t.equal(count, 3, '3 data events')
      graph.close(t.end)
    })
    rs.on('error', function (err) {
      t.error(err, 'should not error')
    })
  })
})

test('.createDiffStream empty diff', function (t) {
  var graph = newDag()
  var empty = newDag()
  writeSomeBros(graph, function () {
    var diff = empty.createDiffStream()
    diff.pipe(graph.createMatchStream()).pipe(diff)
    diff.on('end', function () {
      t.same(diff.since, [], 'empty since')
      t.end()
    })
  })
})

test('.createDiffStream empty match', function (t) {
  var graph = newDag()
  var empty = newDag()
  writeSomeBros(graph, function () {
    var diff = graph.createDiffStream()
    diff.pipe(empty.createMatchStream()).pipe(diff)
    diff.on('end', function () {
      t.same(diff.since, [], 'empty since')
      t.end()
    })
  })
})

test('.createDiffStream same diff and match', function (t) {
  var graph = newDag()
  writeSomeBros(graph, function () {
    var diff = graph.createDiffStream()
    diff.pipe(graph.createMatchStream()).pipe(diff)
    diff.on('end', function () {
      graph.heads(function (_, heads) {
        heads = heads.map(function (node) {
          return node.key
        })
        t.same(diff.since.sort(), heads.sort(), 'empty since')
        t.end()
      })
    })
  })
})

test('.createDiffStream different sets', function (t) {
  var graph = newDag()
  var other = newDag()

  other.append('allen', function () {
    writeSomeBros(graph, function () {
      var diff = graph.createDiffStream()
      diff.pipe(other.createMatchStream()).pipe(diff)
      diff.on('end', function () {
        t.same(diff.since, [], 'empty since')
        t.end()
      })
    })
  })
})

test('.createDiffStream partial match', function (t) {
  var graph = newDag()
  var other = newDag()

  other.append('zuckerberg', function (_, node) {
    writeSomeBros(graph, function () {
      var diff = graph.createDiffStream()
      diff.pipe(other.createMatchStream()).pipe(diff)
      diff.on('end', function () {
        t.same(diff.since, [node.key], 'contains zuck')
        t.end()
      })
    })
  })
})

function writeSomeBros (graph, cb) {
  var ws = graph.createWriteStream()
  var bros = [new Buffer('zuckerberg'), new Buffer('dorsey'), new Buffer('ballmer')]
  for (var i = 0; i < bros.length; i++) ws.write({value: bros[i]})
  ws.end(cb)
}

function newDag () {
  return DAG(memdb())
}

// { key: <Buffer 12 91 e8 9e ee a5 aa e9 bb f8 3e 4d e7 bc 62 77 99 53 1c 47 c5 a8 54 42 c6 82 5e 09 b7 64 fe 91>,
//   links: [],
//   value: <Buffer 7a 75 63 6b 65 72 62 65 72 67>,
//   sort: 0,
//   log: 0,
//   seq: 1 }
