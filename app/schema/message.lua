local spacer = require 'spacer'

spacer:space({
  name = 'message',
  format = {
    { name = 'id', type = 'unsigned' },
    { name = 'node_id', type = 'unsigned' },
    { name = 'offset', type = 'number' },
    { name = 'data', type = 'any', is_nullable = true },
  },
  indexes = {
    { name = 'primary', type = 'tree', unique = true, sequence = true,
      parts = { 'id' } },
  },
  opts = {
    engine = 'vinyl',
    temporary = false,
  }
})

spacer:space({
  name = 'message_index',
  format = {
    { name = 'id', type = 'unsigned' },
    { name = 'node_id', type = 'unsigned' },
    { name = 'offset', type = 'number' },
  },
  indexes = {
    { name = 'primary', type = 'tree', unique = true, sequence = true,
      parts = { 'id' } },
    { name = 'node_id', type = 'tree', unique = false, parts = { 'node_id' } },
    { name = 'node_id_and_id', type = 'tree', unique = true,
      parts = { 'node_id', 'id' } },
    { name = 'node_id_and_offset', type = 'tree', unique = false,
      parts = { 'node_id', 'offset' } },
  },
  opts = {
    engine = 'vinyl',
    temporary = false,
  }
})

spacer:space({
  name = 'message_summary',
  format = {
    { name = 'node_id', type = 'unsigned' },
    { name = 'first_id', type = 'unsigned' },
    { name = 'last_id', type = 'unsigned' },
    { name = 'first_offset', type = 'number' },
    { name = 'last_offset', type = 'number' },
    { name = 'count', type = 'unsigned' },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true, sequence = true,
      parts = { 'node_id' } },
  },
  opts = {
    engine = 'memtx',
    temporary = false,
  }
})
