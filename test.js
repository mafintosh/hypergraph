var DAG = require('./')
var test = require('tape')
var memdb = require('memdb')

test('.append and .get', function (t) {
  var dg = newDag()
  var testBuf = new Buffer('zuckerberg')
  dg.append(testBuf, function (err, node1) {
    if (err) return t.ifError(err, 'should not have err')
    dg.get(node1.key, function (err, node2) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(node1.key.toString(), node2.key.toString(), 'keys match')
      dg.close(t.end)
    })
  })
})

test('.add and .get', function (t) {
  var dg = newDag()
  var testBuf = new Buffer('ballmer')
  dg.add(null, testBuf, function (err, node1) {
    if (err) return t.ifError(err, 'should not have err')
    dg.get(node1.key, function (err, node2) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(node1.key.toString(), node2.key.toString(), 'keys match')
      dg.close(t.end)
    })
  })
})

test('.add and .heads', function (t) {
  var dg = newDag()
  var testBuf = new Buffer('dorsey')
  dg.add(null, testBuf, function (err, node1) {
    if (err) return t.ifError(err, 'should not have err')
    dg.heads(function (err, heads) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(heads.length, 1, 'has 1 head')
      t.equal(node1.key.toString('hex'), heads[0].key.toString('hex'), 'key is head')
      dg.close(t.end)
    })
  })
})

test('.createWriteStream and .count', function (t) {
  var dg = newDag()
  writeSomeBros(dg, function () {
    dg.count(function (err, count) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(count, 3, '3 bros')
      dg.close(t.end)
    })
  })
})

test('.createWriteStream and .createReadStream', function (t) {
  var dg = newDag()
  writeSomeBros(dg, function () {
    var count = 0
    var rs = dg.createReadStream()
    rs.on('data', function (node) {
      count += 1
    })
    rs.on('end', function () {
      t.equal(count, 3, '3 data events')
      dg.close(t.end)
    })
    rs.on('error', function (err) {
      t.error(err, 'should not error')
    })
  })
})

function writeSomeBros (dg, cb) {
  var ws = dg.createWriteStream()
  var bros = [new Buffer('zuckerberg'), new Buffer('dorsey'), new Buffer('ballmer')]
  for (var i = 0; i < bros.length; i ++) ws.write({value: bros[i]})
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
