local reqrep = require 'reqrep'
local rx = require 'rx'
local utils = require 'utils'

local partial = utils.partial

local services = {}


function services.api(config, source)

  local make_call, sink = reqrep.reqrep(source, 'api:rep')

  --- Public API

  local api = {}

  -- echo
  api.echo = partial(make_call, 'echo', 'echo')
  api.error = partial(make_call, 'echo', 'error')
  -- sessions
  api.list_sessions = partial(make_call, 'session:req', 'list_sessions')
  api.rename_session = partial(make_call, 'session:req', 'rename_session')
  -- nodes
  api.add_node = partial(make_call, 'node:req', 'add_node')
  api.enable_node = partial(make_call, 'node:req', 'enable_node')
  api.disable_node = partial(make_call, 'node:req', 'disable_node')
  api.remove_node = partial(make_call, 'node:req', 'remove_node')
  api.list_nodes = partial(make_call, 'node:req', 'list_nodes')
  api.connect_nodes = partial(make_call, 'node:req', 'connect_nodes')
  -- reload
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
  services = services
}
