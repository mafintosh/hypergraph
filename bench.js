var bench = require('fastbench')
var memdb = require('memdb')
var DAG = require('./')

function buildAppend () {
  var dag = DAG(memdb())
  var next = 1

  return append

  function append (done) {
    dag.append(next++ + '', done)
  }
}

function buildWriteMemdb () {
  var db = memdb()
  var next = 1

  return writeMemdb

  function writeMemdb (done) {
    var i = next++ + ''
    db.put(i, i, done)
  }
}


var run = bench([
  buildAppend(),
  buildWriteMemdb()
], 10000)

run(run)
