local api = require 'api'
local rx = require 'rx'
local util = require 'util'

local partial = util.partial

local M = {}


function M.service(config, source, scheduler)

  --- API ACL
  box.once('access:v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
    -- Uncomment this to create user graphka_user
    -- box.schema.user.create('graphka_user', { password = 'graphka_pass' })
    -- box.schema.user.grant('graphka_user', 'read,write,execute', 'universe')
  end)

  --- Public API

  local sink = rx.Subject.create()

  local api_table = app
  api.api(config, api_table, 'app', source)
    :delay(0, scheduler)
    :subscribe(sink)
  api_table.reload = function() return pcall(package.reload) end
  api_table.purge = function() sink:onNext({topic='purge'}) end

  return sink

end


return M
