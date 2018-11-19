local fiber = require 'fiber'
local rx = require 'rx'
local utils = require 'utils'

local M = {}

function M.reqrep(caller, source)

  local requests = {}
  local last_id = 0

  local sink = rx.Subject.create()

  local function make_call(module, method, ...)
    local req_id
    local req
    local args = {...}
    req_id = last_id + 1
    last_id = req_id
    req = {
      req_id = req_id,
      responce = nil,
      cond = fiber.cond()
    }
    requests[tostring(req_id)] = req
    sink:onNext({
      to = module,
      rep_to = caller,
      subject = 'call',
      method = method,
      req_id = req_id,
      args = args,
    })
    if not req.responce then
      req.cond:wait()
    end
    local rep = req.responce
    requests[tostring(req_id)] = nil
    return rep.success, rep.result
  end

  local function on_respond(msg)
    local req = requests[tostring(msg.req_id)]
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
    :filter(function(msg) return msg.subject == 're:call' end)
    :subscribe(on_respond, on_error, on_error)

  return make_call, sink

end

local function call(func, req)

  local success, result = pcall(func, rx.util.unpack(req.args))

  return {
    to = req.rep_to,
    subject = req.subject and 're:'..req.subject or nil,
    req_id = req.req_id,
    success = success,
    result = result
  }
end

local function resolve(func_map, req)
  local func = func_map[req.method]
  func = func or function()
    error('No such method "' .. req.method
          ..  '" in module "' .. req.to .. '"')
  end
  return func
end

function M.dispatch(source, func_map)

  local filtered
  local sink

  filtered = source:filter(function(msg) return msg.subject == 'call' end)
  sink = filtered
    :map(utils.partial(resolve, func_map))
    :zip(filtered)
    :map(call)

  return sink
end

return M
