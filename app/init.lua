local fiber = require 'fiber'
local conf = require 'config'
local log = require 'log'
local rx = require 'rx'
local utils = require 'utils'


local modules = utils.merge_tables(
    require 'modules.echo' .modules,
    require 'modules.session' .modules,
    require 'modules.node' .modules,
    require 'modules.api' .modules
)

local default_config = {
  migrations = './migrations',
}


local app = {
  hub = rx.Subject.create(),
  config = {},
}

function app.init(config)
  log.info('app "graphka" init')

  app.config = utils.merge_tables(default_config, config)
  box.spacer = require 'spacer'({
      migrations = app.config.migrations,
  })
  require 'schema'
  if app.config.automigrate then
    box.spacer:migrate_up()
  end

  local hub = app.hub
  local sink
  local source = hub
  for name, mod in pairs(modules) do
    log.info('module "' .. name .. '" init')
    sink = mod(app.config, source)
    if sink then
      sink:subscribe(hub)
    end
  end

  --- debug
  app.hub:dump('hub', require('json').encode)
  app.hub:onNext({'hello', 'world'})

end

function app.destroy()
  log.info('app "graphka" destroy')
  app.hub:onCompleted()
end


if package.reload then
  package.reload:register(app)
end
rawset(_G, 'app', app)
return app
