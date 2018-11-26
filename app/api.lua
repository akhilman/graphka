local fiber = require 'fiber'
local log = require 'log'
local rx = require 'rx'
local util = require 'util'

local assertup = util.assertup

local M = {}


function M.api(api_table, api_topic, source)

  assertup(type(api_table) == 'table', 'api_table should be table')
  assertup(type(api_topic) == 'string', 'api_topic should be string')
  assertup(source.subscribe, 'source should be Observable')

  local result_topic = api_topic .. '_result'
  local publish_topic = api_topic .. '_publish'

  -- Call

  local requests = {}
  local last_id = 0

  local sink = rx.Subject.create()

  local function make_call(call_topic, method, ...)
    local call_id
    local req
    local args = {...}
    call_id = last_id + 1
    last_id = call_id
    req = {
      call_id = call_id,
      responce = nil,
      cond = fiber.cond()
    }
    requests[tostring(call_id)] = req
    sink:onNext({
      topic = call_topic,
      result_topic = result_topic,
      method = method,
      call_id = call_id,
      args = args,
    })
    if not req.responce then
      req.cond:wait()
    end
    local rep = req.responce
    requests[tostring(call_id)] = nil
    return rep.success, rep.result
  end

  local function on_result(msg)
    local req = requests[tostring(msg.call_id)]
    if req then
      req.responce = msg
      req.cond:signal()
    end
  end

  local function on_error(...)
    local args = rx.util.pack(...)
    for _, req in pairs(requests) do
      if not req.responce then
        req.responce = {...}
        req.cond:signal()
      end
    end
  end

  source
    :filter(function(msg)
      return msg.topic == result_topic
    end)
    :subscribe(on_result, on_error, on_error)

  -- Publish

  local function on_publish(msg)
    local method = msg.method
    local call_topic = msg.call_topic
    log.info(string.format('Publishing method "%s" to %s', method, api_topic))
    api_table[method] = function(...)
      return make_call(call_topic, method, ...)
    end
  end

  source
    :filter(function(msg)
      return msg.topic == publish_topic
    end)
    :subscribe(on_publish)

  return sink

end

local function call(func, req)

  local success, result = pcall(func, rx.util.unpack(req.args))

  return {
    topic = req.result_topic,
    call_id = req.call_id,
    success = success,
    result = result
  }
end

local function resolve(call_topic, func_map, req)
  local func = func_map[req.method]
  func = func or function()
    error('No such method "' .. req.method
          ..  '" in module "' .. call_topic .. '"')
  end
  return func
end

function M.publish(func_map, module_topic, api_topic, source)

  assertup(type(func_map) == 'table', 'func_map should be table')
  assertup(type(module_topic) == 'string', 'call_topic should be string')
  assertup(type(api_topic) == 'string', 'api_topic should be string')
  assertup(source.subscribe, 'source should be Observable')

  local call_topic = module_topic .. '_call'
  local publish_topic = api_topic .. '_publish'

  local filtered = source:filter(function(msg)
    return msg.topic == call_topic
  end)
  local call_sink = filtered
    :filter(function(msg) return msg.topic == call_topic end)
    :map(util.partial(resolve, module_topic, func_map))
    :zip(filtered)
    :map(call)

  local publish_sink = source
    :filter(function(msg) return msg.topic == 'ready' end)
    :map(function()
      return rx.Observable.fromTable(func_map, pairs, true)
    end)
    :flatMap()
    :map(function(func, name)
      return {
        topic = publish_topic,
        call_topic = call_topic,
        method = name,
      }
    end)

  return call_sink:merge(publish_sink)
end

return M
