local reqrep = require 'reqrep'
local rx = require 'rx'
local util = require 'util'

local partial = util.partial

local services = {}


function services.api(config, source, scheduler)

  local make_call, sink = reqrep.reqrep(source, 'api_rep')

  --- Public API

  local api = {}

  -- echo
  api.echo = partial(make_call, 'echo', 'echo')
  api.error = partial(make_call, 'echo', 'error')
  -- session
  api.list_sessions = partial(make_call, 'session_req', 'list_sessions')
  api.rename_session = partial(make_call, 'session_req', 'rename_session')
  -- node
  api.add_node = partial(make_call, 'node_req', 'add_node')
  api.enable_node = partial(make_call, 'node_req', 'enable_node')
  api.disable_node = partial(make_call, 'node_req', 'disable_node')
  api.remove_node = partial(make_call, 'node_req', 'remove_node')
  api.list_nodes = partial(make_call, 'node_req', 'list_nodes')
  api.connect_nodes = partial(make_call, 'node_req', 'connect_nodes')
  api.disconnect_nodes = partial(make_call, 'node_req', 'disconnect_nodes')
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
