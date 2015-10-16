# dat-graph

WORK IN PROGRESS

```
npm install dat-graph
```

## API

### var dg = graph(levelup)

creates a new graph from a levelup

### Range Options

Various methods use these options for specifying ranges in the graph:

**since** - array of nodes to begin the range
**until** - array of nodes to end the range

### dg.range(opts, cb)

Returns the nodes between the ranges you pass in to `opts`

`opts` should be Range Options

All nodes between `since` and `until` will be returned to `cb`

### dg.count(opts, cb)

Get the total number of nodes between a range. Calls `cb` with the node count for the range you specify

`opts` should be Range Options.

### dg.match(hashes, cb)

Given an array of hashes, calls `cb` with the hashes that exist in the local graph

### dg.heads(cb)

Calls `cb` with an array of the current heads of the graph

### dg.createReadStream(opts)

Returns a readable stream that will emit graph nodes. `opts` should be Range Options

### dg.createWriteStream()

Returns a writable stream that stores data in the graph

### dg.get(key, cb)

Gets the value for a key in the graph, calls `cb` with the value

### dg.append(value, cb)

Appends `value` to the current head, calls `cb` when done. Uses `.add` internally.

### dg.add(links, value, cb)

Inserts `value` into the graph as a child of `links`, calls `cb` when done.

### dg.close(cb)

Calls close on the underlying `dg.db`

## License

MIT
