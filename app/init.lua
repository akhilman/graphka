local fiber = require 'fiber'
local conf = require 'config'
local log = require 'log'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'
local services = require 'services'

local default_config = {
  migrations = './migrations',
}

local app = {
  scheduler = rxtnt.FiberScheduler.create(),
  hub = rx.BehaviorSubject.create(),
  config = {},
}

function app.init(config)
  log.info('app "graphka" init')

  app.config = util.merge_tables(default_config, config)
  box.spacer = require 'spacer'({
      migrations = app.config.migrations,
      automigrate = app.config.automigrate
  })
  require 'schema'

  local on_service_error
  local init_service
  function init_service(name, serv)
    log.info('service "' .. name .. '" init')
    local sink = serv(app.config, app.hub, app.scheduler)
    if sink then
      sink:catch(util.partial(on_service_error, name, serv)):subscribe(app.hub)
    end
  end
  function on_service_error(name, serv, err)
    log.error('Error in service "' .. name .. '": ' .. err)
    fiber.sleep(1)
    return init_service(name, serv)
  end
  for name, serv in pairs(services.services) do
    init_service(name, serv)
  end

  --- debug
  app.hub:dump('hub', require('json').encode)
  app.hub:onNext({'hello', 'world'})

end

function app.destroy()
  log.info('app "graphka" destroy')
  app.hub:onCompleted()
  fiber.sleep(0.1)
end


if package.reload then
  package.reload:register(app)
end
rawset(_G, 'app', app)
return app
