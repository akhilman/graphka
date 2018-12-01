local rx = require 'rx'
local fun = require 'fun'

local M = {}

function M.assertup(v, message, level)
  level = level or 2
  message = message or 'assertion failed!'
  if not v then
    error(message, level + 1)
  end
end

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

function M.revpartial(func, ...)
  local post_args = rx.util.pack(...)
  return function(...)
    local new_args = rx.util.pack(...)
    local args = M.concatenate(new_args, post_args)
    return func(rx.util.unpack(args))
  end
end

function M.itemgetter(item)
  return function(table)
    return table[item]
  end
end

function M.itemeq(item, value)
  return function(table)
    return table[item] == value
  end
end

function M.take_n_args(n, ...)
  return rx.util.unpack(
    fun.totable(
      fun.take_n(
        n,
        fun.iter(
          rx.util.pack(...)
  ))))
end

function M.snake_to_camel(txt)
  local function camel(txt)
    local s, e, c = string.find(txt, '_(%l)')
    if not c then
      return txt
    end
    txt = string.sub(txt, 1, s-1) .. string.upper(c) .. string.sub(txt, e+1)
    return camel(txt)
  end
  return camel('_' .. txt)
end

function M.camel_to_snake(txt)
  local s, e, c = string.find(txt, '(%u)')
  if not c then
    return txt
  end
  local pre = s > 1 and string.sub(txt, 1, s-1) .. '_' or ''
  txt = pre .. string.lower(c) .. string.sub(txt, e+1)
  return M.camel_to_snake(txt)
end

return M
