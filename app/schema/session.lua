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
