var protobuf = require('protocol-buffers')

module.exports = protobuf([
  'message Node {',
  '  required bytes key = 3;',
  '  repeated bytes links = 1;',
  '  optional bytes value = 2;',
  '  optional uint64 sort = 6;',
  '  required uint64 path = 4;',
  '  required uint64 seq = 5;',
  '}',
  'message WireNode {',
  '  repeated bytes links = 1;',
  '  optional bytes value = 2;',
  '}',
  'message Question {',
  '  required uint32 id = 1;',
  '  repeated bytes keys = 2;',
  '}',
  'message Answer {',
  '  required uint32 id = 1;',
  '  repeated uint32 matches = 2;',
  '}'
].join('\n'))
