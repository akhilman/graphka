local util = require 'util'
local rx = require 'rx'

local assertup = util.assertup

local M = {}

--- @class Record

local Record = {}

function Record:__index(key)
  local fields = rawget(getmetatable(self), '_fields')
  key = type(key) == 'number' and fields[key] or key
  if fun.index(key, fields) then
    return rawget(self, key)
  else
    local v = getmetatable(self)[key]
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

function Record:__len()
  return #self._fields
end

function Record:__tostring()
  return string.format("Record(%s)", self._schema)
end

function Record.new(schema)

  local New

  assert(F[schema], string.format('No such schema "%s"', schema))
  local fields = fun.iter(F[schema]):map(function(k, v) return k end):totable()
  table.sort(fields, function(a, b) return F[schema][a] < F[schema][b] end)

  local to_tuple = T[schema].tuple
  local from_tuple = T[schema].dict

  New = {}
  New._schema = schema
  New._fields = fields
  New.__len = rx.util.constant(#fields)
  New.__tostring = rx.util.constant(util.snake_to_camel(schema))
  New.__index = Record.__index
  New.__newindex = Record.__newindex

  New.create = function(...)
    return setmetatable(from_tuple({...}), New)
  end

  New.from_tuple = function(table)
    return setmetatable(from_tuple(table), New)
  end

  New.from_map = function(table)
    return New.from_tuple(to_tuple(table))
  end

  New.to_tuple = to_tuple

  New.to_map = function(self)
    return from_tuple(self:to_tuple())
  end

  New.copy = function(self)
    return New.from_tuple(self:to_tuple())
  end

  setmetatable(New, Record)

  return New
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
  val = Record.new(util.camel_to_snake(key))
  rawset(M, key, val)
  return val
end

setmetatable(M, M)

return M
