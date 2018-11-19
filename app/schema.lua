local spacer = require 'spacer'

spacer:space({
    name = 'clients',
    format = {
        { name = 'client_id', type = 'unsigned' },
        { name = 'name', type = 'string', is_nullable=true },
        { name = 'atime', type = 'number' },
    },
    indexes = {
        { name = 'primary', type = 'hash', unique = true, parts = { 'client_id' } },
        { name = 'atime', type = 'tree', unique = false, parts = { 'atime' } },
    },
    opts = {
        engine = 'memtx',
        temporary = true,
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
