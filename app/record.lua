local util = require 'util'
local rx = require 'rx'

--- @class Record

local Record = {}

function Record:__index(key)
  local mt = getmetatable(self)
  local ix = type(key) == 'number' and key <= #self._fields and key
             or fun.index(key, self._fields)
  if ix then
    return rawget(self, ix)
  else
    local v = mt[key]
    if v then
      return v
    end
  end
  error(string.format('No such field "%s"', key))
end

function Record:__newindex(key, value)
  local ix = type(key) == 'number' and key <= #self._fields and key
             or fun.index(key, self._fields)
  assert(ix, string.format('No such field "%s"', key))
  return rawset(self, ix, value)
end

function Record:__len()
  return #self._fields
end

function Record:__tostring()
  return string.format("Record(%s)", self._schema)
end

function Record.create(schema, ...)
  assert(type(schema) == 'string', 'schema must be a string')
  local fields = fun.iter(F[schema]):map(function(k, v) return k end):totable()
  table.sort(fields, function(a, b) return F[schema][a] < F[schema][b] end)
  local args = {...}
  local record = {}
  record._schema = schema
  record._fields = fields
  for n=1, math.min(#fields, #args), 1 do
    record[n] = args[n]
  end
  return setmetatable(record, Record)
end

function Record.from_tuple(schema, tuple)
  local record = Record.create(schema)
  for n, field in ipairs(record._fields) do
    record[n] = tuple[n]
  end
  return record
end

function Record.from_table(schema, table)
  local record = Record.create(schema)
  for n, field in ipairs(record._fields) do
    record[n] = table[field]
  end
  return record
end

function Record:to_map()
  assert(type(self._schema) == 'string', 'schema attribute is not defined')
  local table = {}
  for n, field in ipairs(self._fields) do
    table[field] = self[n]
  end
  return table
end

function Record:to_tuple()
  assert(type(self._schema) == 'string', 'schema attribute is not defined')
  local tuple = {}
  local len = 0
  for n, field in ipairs(self._fields) do
    tuple[n] = self[n]
  end
  return tuple
end

function Record:unpack()
  return rx.util.unpack(self:to_tuple())
end

return Record
