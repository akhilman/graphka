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

function M.partial(func, ...)
  local prefix_args = rx.util.pack(...)
  return function(...)
    local args = fun.totable(fun.chain(prefix_args, {...}))
    return func(rx.util.unpack(args))
  end
end

return M
