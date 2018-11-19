local reqrep = require 'reqrep'
local rx = require 'rx'
local utils = require 'utils'

local modules = {}


function modules.api(config, source)

  local make_call, sink = reqrep.reqrep('api', source)

  --- Public API

  local api = {}

  api.echo = utils.partial(make_call, 'echo', 'echo')
  api.reload = function() return pcall(package.reload) end

  rawset(_G, 'graphka', api)

  --- API ACL
  box.once('access:v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
    -- Uncomment this to create user graphka_user
    -- box.schema.user.create('graphka_user', { password = 'graphka_pass' })
    -- box.schema.user.grant('graphka_user', 'read,write,execute', 'universe')
  end)

  return sink

end


return {
  modules = modules
}
