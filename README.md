# hypergraph

A Merkle DAG

```
npm install hypergraph
```

[![build status](http://img.shields.io/travis/mafintosh/hypergraph.svg?style=flat)](http://travis-ci.org/mafintosh/hypergraph)

## API

### var graph = hypergraph(levelup)

creates a new graph from a levelup

### Range Options

Various methods use these options for specifying ranges in the graph:

**since** - array of nodes to begin the range
**until** - array of nodes to end the range

### graph.count(opts, cb)

Get the total number of nodes between a range. Calls `cb` with the node count for the range you specify

`opts` should be Range Options.

### graph.match(hashes, cb)

Given an array of hashes, calls `cb` with the hashes that exist in the local graph

### graph.heads(cb)

Calls `cb` with an array of the current heads of the graph

### graph.createReadStream(opts)

Returns a readable stream that will emit graph nodes. `opts` should be Range Options

### graph.createWriteStream()

Returns a writable stream that stores data in the graph

### graph.get(key, cb)

Gets the value for a key in the graph, calls `cb` with the value

### graph.append(value, cb)

Appends `value` to the current head, calls `cb` when done. Uses `.add` internally.

### graph.add(links, value, cb)

Inserts `value` into the graph as a child of `links`, calls `cb` when done.

### graph.close(cb)

Calls close on the underlying `graph.db`

## License

MIT
