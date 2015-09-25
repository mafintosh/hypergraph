# dat-graph

WORK IN PROGRESS

```
npm install dat-graph
```

## Usage

``` js
var datGraph = require('dat-graph')
var graph = datGraph(db) // db is a levelup instance

var stream = graph.addStream([], function (err, node) {
  console.log('inserted', node) // {links: [], value: (hash of the below writes)}

  var anotherStream = graph.addStream([node], function (err) {

  })

  anotherStream.write(new Buffer('test'))
})

stream.write(new Buffer('entry one'))
stream.write(new Buffer('entry two'))
stream.end()
```

The above results in

```
node #2 (-> hash of "test")
 |
node #1 (-> hash of ("entry one", "entry two"))
```

``` js
var stream = graph.getStream(nodeHash)

stream.on('data', function (data) {
  // data is entry one, then entry two
})

var stream = graph.nodeStream()

stream.on('data', function (node) {
  // returns node #1, node #2
})

graph.heads(function (err, heads) {
  // returns the heads of the graph
})

graph.get(nodeHash, function (err, node) {
  // returns the graph node
})

var stream = graph.replicate({mode: 'push'})

stream.on('end', function () {
  var stream = graph.eventStream({since: 5})

  stream.on('data', function (data) {
    // {start: date, end: date, sen}
  })
})

```

## On disk representation

```
# append only log (to support changes feed)
changes!1 => node-hash-1
changes!2 => node-hash-2

# graph nodes
nodes!node-hash-1
nodes!node-hash-2  => {links: [node-hash-1], value: {type: whatever, commit: commit-hash-2}}

# commits (child of a graph node)
commits!commit-hash-2!1!test
commits!commit-hash-1!1!entry-one
commits!commit-hash-1!2!entry-two
```

## License

MIT
