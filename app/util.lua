local rx = require 'rx'
local fun = require 'fun'

local M = {}

function M.merge_tables(...)

  local ret = {}
  local sources = {...}

  for _, src in ipairs(sources) do
    for k, v in pairs(src) do
      ret[k] = v
    end
  end

  return ret
end

function M.concatenate(...)

  local ret = {}
  local sources = {...}

  for _, src in ipairs(sources) do
    for _, v in ipairs(src) do
      table.insert(ret, v)
    end
  end

  return ret
end

function M.partial(func, ...)
  local pre_args = rx.util.pack(...)
  return function(...)
    local new_args = rx.util.pack(...)
    local args = M.concatenate(pre_args, new_args)
    return func(rx.util.unpack(args))
  end
end

function M.itemgetter(item)
  return function(table)
    return table[item]
  end
end

return M
