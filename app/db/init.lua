local util = require 'util'

local M = {}
M =  util.merge_tables(
  require 'db.session',
  require 'db.node'
  -- require 'db.message'
)

return M
