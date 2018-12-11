local util = require 'util'
local rx = require 'rx'
local msgpack = require 'msgpack'

local NULL = msgpack.NULL
local assertup = util.assertup

local M = {}

--- @class Record

local function new_record(schema)

  local Record

  assert(F[schema], string.format('No such schema "%s"', schema))
  local fields = fun.iter(F[schema]):map(function(k, v) return k end):totable()
  table.sort(fields, function(a, b) return F[schema][a] < F[schema][b] end)

  local to_tuple = T[schema].tuple
  local from_tuple = T[schema].dict

  Record = {}
  Record._schema = schema
  Record._fields = fields
  Record.__len = rx.util.constant(#fields)
  Record.__tostring = rx.util.constant(util.snake_to_camel(schema))

  function Record:__index(key)
    local fields = rawget(getmetatable(self), '_fields')
    key = type(key) == 'number' and fields[key] or key
    if fun.index(key, fields) then
      return NULL
    else
      local v = rawget(getmetatable(self), key)
      if v then
        return v
      end
    end
    error(string.format('No such field "%s"', key), 2)
  end

  function Record:__newindex(key, value)
    local fields = rawget(getmetatable(self), '_fields')
    key = type(key) == 'number' and fields[key] or key
    if not fun.index(key, fields) then
      error(string.format('No such field "%s"', key), 2)
    end
    return rawset(self, key, value)
  end

  Record.create = function(...)
    return setmetatable(from_tuple({...}), Record)
  end

  Record.from_tuple = function(table)
    return setmetatable(from_tuple(table), Record)
  end

  Record.from_map = function(table)
    return Record.from_tuple(to_tuple(table))
  end

  Record.to_tuple = to_tuple

  Record.to_map = function(self)
    return from_tuple(self:to_tuple())
  end

  Record.copy = function(self)
    return Record.from_tuple(self:to_tuple())
  end

  setmetatable(Record, Record)

  return Record
end

--- Module

M.__call = function(self, schema)
  return self[util.snake_to_camel(schema)]
end

M.__index = function(self, key)
  local val = rawget(M, key)
  if val then
    return val
  end
  assertup(string.match(key, '^%u'),
           'Record classs name should starat with uppercase character')
  val = new_record(util.camel_to_snake(key))
  rawset(M, key, val)
  return val
end

setmetatable(M, M)

return M
