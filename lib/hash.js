var framedHash = require('framed-hash')

module.exports = function hash (node) {
  var sha = framedHash('sha256')
  sha.update(node.value)
  for (var i = 0; i < node.links.length; i++) sha.update(node.links[i])
  return sha.digest()
}
