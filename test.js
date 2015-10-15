var test = require('tape')
var level = require('level')
var rimraf = require('rimraf')
var DAG = require('./')

rimraf.sync('./testdb')
var db = level('./testdb')
var dg = DAG(db)

test('.append and .get', function (t) {
  var testBuf = new Buffer('zuckerberg')
  dg.append(testBuf, function (err, node1) {
    if (err) return t.ifError(err, 'should not have err')
    dg.get(node1.key, function (err, node2) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(node1.key.toString(), node2.key.toString(), 'keys match')
      t.end()
    })
  })
})

test('.add and .get', function (t) {
  var testBuf = new Buffer('ballmer')
  dg.add(null, testBuf, function (err, node1) {
    if (err) return t.ifError(err, 'should not have err')
    dg.get(node1.key, function (err, node2) {
      if (err) return t.ifError(err, 'should not have err')
      t.equal(node1.key.toString(), node2.key.toString(), 'keys match')
      t.end()
    })
  })
})

// { key: <Buffer 12 91 e8 9e ee a5 aa e9 bb f8 3e 4d e7 bc 62 77 99 53 1c 47 c5 a8 54 42 c6 82 5e 09 b7 64 fe 91>,
//   links: [],
//   value: <Buffer 7a 75 63 6b 65 72 62 65 72 67>,
//   sort: 0,
//   log: 0,
//   seq: 1 }
