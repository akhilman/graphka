local spacer = require 'spacer'

spacer:space({
  name = 'sessions',
  format = {
    { name = 'id', type = 'unsigned' },
    { name = 'name', type = 'string', is_nullable = true },
    { name = 'peer', type = 'string', is_nullable = true },
    { name = 'ctime', type = 'number' },
  },
  indexes = {
    { name = 'primary', type = 'hash', unique = true, parts = { 'id' } },
  },
  opts = {
    engine = 'memtx',
    temporary = true,
  }
})
