local spacer = require 'spacer'

spacer:space({
  name = 'sessions',
  format = {
    { name = 'id', type = 'unsigned' },
    { name = 'name', type = 'string', is_nullable = true },
    { name = 'peer', type = 'string', is_nullable = true },
    { name = 'atime', type = 'number' },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true, parts = { 'id' } },
    { name = 'atime', type = 'tree', unique = false, parts = { 'atime' } },
  },
  opts = {
    engine = 'memtx',
    temporary = true,
  }
})

spacer:space({
  name = 'nodes',
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
  name = 'node_links',
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

--[[
spacer:space({
  name = 'message_data',
  format = {
    { name = 'message_id', type = 'unsigned' },
    { name = 'node_id', type = 'unsigned' },
    { name = 'data', is_nullable = True },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true, parts = { 'message_id' } },
    { name = 'node_id', type = 'tree', unique = false, parts = { 'node_id' } },
  },
  opts = {
    engine = 'vinyl',
  }
})
--]]
