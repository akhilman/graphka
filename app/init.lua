local fiber = require 'fiber'
local conf = require 'config'
local log = require 'log'
local rx = require 'rx'
local utils = require 'utils'


local services = utils.merge_tables(
    require 'services.echo' .services,
    require 'services.session' .services,
    require 'services.node' .services,
    require 'services.api' .services
)

local default_config = {
  migrations = './migrations',
}


local app = {
  hub = rx.BehaviorSubject.create(),
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
  local source = hub
  local on_service_error
  local init_service
  function init_service(name, serv)
    log.info('service "' .. name .. '" init')
    local sink = serv(app.config, source)
    if sink then
      sink:catch(utils.partial(on_service_error, name, serv)):subscribe(hub)
    end
  end
  function on_service_error(name, serv, err)
    log.error('Error in service "' .. name .. '": ' .. err)
    fiber.sleep(1)
    return init_service(name, serv)
  end
  for name, serv in pairs(services) do
    init_service(name, serv)
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
