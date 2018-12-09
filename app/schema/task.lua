local spacer = require 'spacer'

spacer:space({
  name = 'task',
  format = {
    { name = 'id', type = 'unsigned' },
    { name = 'node_id', type = 'unsigned' },
    { name = 'session_id', type = 'unsigned' },
    { name = 'offset', type = 'number' },
    { name = 'expires', type = 'number' },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true, sequence=true,
      parts = { 'id' } },
    { name = 'node_id', type = 'hash', unique = true, parts = { 'node_id' } },
    { name = 'session_id', type = 'tree', unique = false,
      parts = { 'session_id' } },
    { name = 'expires', type = 'tree', unique = false, parts = { 'expires' } },
  },
  opts = {
    engine = 'memtx',
    temporary = true,
  }
})

spacer:space({
  name = 'ack_offset',
  format = {
    { name = 'node_id', type = 'unsigned' },
    { name = 'offset', type = 'number' },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true, parts = { 'node_id' } },
  },
  opts = {
    engine = 'memtx',
    temporary = false,
  }
})

