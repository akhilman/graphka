local fiber = require 'fiber'
local rx = require 'rx'
local utils = require 'utils'

local modules = {}


function modules.api(config, source)

  local requests = {}
  local last_id = 0

  local sink = rx.Subject.create()

  local function make_request(module, subject, ...)
    local req_id
    local req
    local args = {...}
    last_id = last_id + 1
    req_id = tostring(last_id)
    req = {
      req_id = req_id,
      responce = nil,
      cond = fiber.cond()
    }
    requests[req_id] = req
    sink:onNext({
      to = module,
      subject = subject,
      req_id = req_id,
      args = args,
    })
    if not req.responce then
      req.cond:wait()
    end
    local rep = req.responce
    return rep.success, rep.result
  end

  local function on_respond(msg)
    print('on_respond', msg)
    local req = requests[msg.req_id]
    req.responce = msg
    req.cond:signal()
  end

  source:subscribe(on_respond)

  --- Public API

  local api = {}

  api.echo = utils.partial(make_request, 'echo', 'echo')
  api.reload = function() return pcall(package.reload) end

  for k, v in pairs(api) do
    rawset(_G, k, v)
  end

  --- API ACL
  box.once('access:v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
    -- Uncomment this to create user graphka_user
    -- box.schema.user.create('graphka_user', { password = 'graphka_pass' })
    -- box.schema.user.grant('graphka_user', 'read,write,execute', 'universe')
  end)

  return sink

end


return modules
