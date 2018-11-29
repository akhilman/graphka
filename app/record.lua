local util = require 'util'
local rx = require 'rx'

local assertup = util.assertup

local M = {}

--- @class Record

local Record = {}

local cached_classes = {}

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

  New = cached_classes[schema]
  if New then
    return New
  end

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

  cached_classes[schema] = New

  return New
end

--- Module

M.__call = function(self, schema)
  return Record.new(schema)
end

M.__index = function(self, key)
  local v = rawget(M, key)
  if not v then
    v = Record.new(util.camel_to_snake(key))
  end
  return v
end

setmetatable(M, M)

return M

--[[
local classes = fun.iter(F)
  :map(util.partial(util.take_n_args, 1))
  :map(util.revpartial(string.match, '^%l'))
  :filter(fun.operator.truth)
  :map(function(v) return util.snake_to_camel(v), make_record(v) end)
  :tomap()

M = util.merge_tables(M, classes)
]]--
