local db = require 'db'
local fiber = require 'fiber'
local log = require 'log'
local rx = require 'rx'
local util = require 'util'

local assertup = util.assertup

local M = {}


function M.api(config, api_table, api_topic, source)

  assertup(type(api_table) == 'table', 'api_table should be table')
  assertup(type(api_topic) == 'string', 'api_topic should be string')
  assertup(source.subscribe, 'source should be Observable')

  local result_topic = api_topic .. '_result'
  local publish_topic = api_topic .. '_publish'

  -- Call

  local pending_calls = {}
  local last_id = 0

  local sink = rx.Subject.create()

  local function make_call(call_topic, method, ...)
    local call_id
    local call
    local session_id = box.session.id()
    assert(db.session.exist(session_id),
           string.format('Session #%d not exist', session_id))
    local args = {...}
    call_id = last_id + 1
    last_id = call_id
    call = {
      call_id = call_id,
      result = nil,
      cond = fiber.cond()
    }
    pending_calls[tostring(call_id)] = call
    sink:onNext({
      topic = call_topic,
      result_topic = result_topic,
      method = method,
      call_id = call_id,
      args = args,
      session_id = session_id
    })
    if not call.result then
      local ok = call.cond:wait(config.timeout + 1)
      if not ok then
        call.result = { success = false, result = 'Call timeout' }
      end
    end
    local msg = call.result
    pending_calls[tostring(call_id)] = nil
    return msg.success, msg.result
  end

  local function on_result(msg)
    local call = pending_calls[tostring(msg.call_id)]
    if call then
      call.result = msg
      call.cond:signal()
    end
  end

  local function on_error(...)
    local args = rx.util.pack(...)
    for _, call in pairs(pending_calls) do
      if not call.result then
        call.result = {...}
        call.cond:signal()
      end
    end
  end

  source
    :filter(util.itemeq('topic', result_topic))
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
    :filter(util.itemeq('topic', publish_topic))
    :subscribe(on_publish)

  return sink

end

local function call(func, req, pass_call)

  local args = req.args
  if pass_call then
    args = util.concatenate({req}, args)
  end
  local success, result = pcall(func, rx.util.unpack(args))

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

function M.publish(func_map, module_topic, api_topic, source, pass_call)

  assertup(type(func_map) == 'table', 'func_map should be table')
  assertup(type(module_topic) == 'string', 'call_topic should be string')
  assertup(type(api_topic) == 'string', 'api_topic should be string')
  assertup(source.subscribe, 'source should be Observable')

  local call_topic = module_topic .. '_call'
  local publish_topic = api_topic .. '_publish'

  local filtered = source:filter(util.itemeq('topic', call_topic))
  local call_sink = filtered
    :filter(function(msg) return msg.topic == call_topic end)
    :map(util.partial(resolve, module_topic, func_map))
    :zip(filtered)
    :map(util.revpartial(call, pass_call))

  local publish_sink = source
    :filter(util.itemeq('topic', 'setup'))
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
