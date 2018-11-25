local util = require 'util'
local rx = require 'rx'

--- @class Record

local Record = {}

local cached_fields = {}
local function get_fields(schema)
  local fields = cached_fields[schema]
  if fields then
    return fields
  end
  assert(F[schema], string.format('No such schema "%s"', schema))
  local fields = fun.iter(F[schema]):map(function(k, v) return k end):totable()
  table.sort(fields, function(a, b) return F[schema][a] < F[schema][b] end)
  cached_fields[schema] = fields
  return fields
end

function Record:__index(key)
  local fields = get_fields(self._schema)
  key = type(key) == 'number' and fields[key] or key

  if key == '_schema' or fun.index(key, fields) then
    return rawget(self, key)
  else
    local v = getmetatable(self)[key]
    if v then
      return v
    end
  end
  error(string.format('No such field "%s"', key))
end

function Record:__newindex(key, value)
  local fields = get_fields(self._schema)
  key = type(key) == 'number' and fields[key] or key
  assert(fun.index(key, fields), string.format('No such field "%s"', key))
  return rawset(self, key, value)
end

function Record:__len()
  return #get_fields(self._schema)
end

function Record:__tostring()
  return string.format("Record(%s)", self._schema)
end

function Record.create(schema, ...)
  local record = {}
  record._schema = schema
  if select('#', ...) > 0 then
    local fields = get_fields(schema)
    for n=1, math.min(#fields, select('#', ...)), 1 do
      record[fields[n]] = select(n, ...)
    end
  end
  return setmetatable(record, Record)
end

function Record.from_map(schema, table)
  local fields = get_fields(schema)
  local record = Record.create(schema)
  for n, field in ipairs(fields) do
    record[field] = table[field]
  end
  return record
end

function Record.from_tuple(schema, tuple)
  local fields = get_fields(schema)
  local record = Record.create(schema)
  for n, field in ipairs(fields) do
    record[field] = tuple[n]
  end
  return record
end

function Record:copy()
  return Record.from_map(self._schema, self:to_map())
end

function Record:to_map()
  local fields = get_fields(self._schema)
  local table = {}
  for n, field in ipairs(fields) do
    table[field] = self[field]
  end
  return table
end

function Record:to_tuple()
  -- TODO use space.frommap() in tarantool 1.10
  -- local space = box.space[self._schema]
  -- return space:frommap(self)
  local fields = get_fields(self._schema)
  local tuple = {}
  local len = 0
  for n, field in ipairs(fields) do
    tuple[n] = self[field]
  end
  return tuple
end

function Record:unpack()
  return rx.util.unpack(self:to_tuple())
end

function Record:get_fields()
  return get_fields(self._schema)
end

return Record
