local spacer = require 'spacer'

spacer:space({
  name = 'node',
  format = {
    { name = 'id', type = 'unsigned' },
    { name = 'name', type = 'string' },
    { name = 'enabled', type = 'boolean' },
    { name = 'nice', type = 'integer' },
    { name = 'start_offset', type = 'number' },
    { name = 'history_size', type = 'integer' },
    { name = 'tmp_session_id', type = 'unsigned', is_nullable = true },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true, parts = { 'id' },
      sequence = true },
    { name = 'name', type = 'hash',  unique = true, parts = { 'name' } },
    { name = 'tmp_session_id', type = 'tree', unique = false,
      parts = { 'tmp_session_id' } },
  },
  opts = {
    engine = 'memtx',
  }
})

spacer:space({
  name = 'wire',
  format = {
    { name = 'id', type = 'unsigned' },
    { name = 'source_id', type = 'unsigned' },
    { name = 'sink_id', type = 'unsigned' },
    { name = 'source_required', type = 'boolean' },
    { name = 'sink_required', type = 'boolean' },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true, parts = { 'id' },
      sequence = true },
    { name = 'source_id', type = 'tree', unique= false,
      parts = { 'source_id' } },
    { name = 'sink_id', type = 'tree', unique = false,
      parts = { 'sink_id' } },
    { name = 'soruce_and_sink_id', type = 'hash', unique = true,
      parts = { 'source_id', 'sink_id' } },
  },
  opts = {
    engine = 'memtx',
  }
})
