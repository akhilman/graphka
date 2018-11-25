local spacer = require 'spacer'

spacer:space({
  name = 'node',
  format = {
    { name = 'id', type = 'unsigned' },
    { name = 'name', type = 'string' },
    { name = 'enabled', type = 'boolean' },
    { name = 'priority', type = 'integer' },
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
    { name = 'input_id', type = 'unsigned' },
    { name = 'output_id', type = 'unsigned' },
    { name = 'input_required', type = 'boolean' },
    { name = 'output_required', type = 'boolean' },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true,
      parts = { 'input_id', 'output_id' } },
    { name = 'input_id', type = 'tree', unique= false,
      parts = { 'input_id' } },
    { name = 'output_id', type = 'tree', unique = false,
      parts = { 'output_id' } },
  },
  opts = {
    engine = 'memtx',
  }
})
