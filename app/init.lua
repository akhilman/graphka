local fiber = require 'fiber'
local conf = require 'config'
local log = require 'log'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'

local default_config = {
  migrations = './migrations',
  timeout = 10,
  services = {
    'api',
    'session',
    'node',
    'message',
    'test'
  }
}

local app = {
  config = {},
}

function app.init(config)
  log.info('app "graphka" init')

  app.config = util.merge_tables(default_config, config)
  app.scheduler = rxtnt.FiberScheduler.create()
  app.hub = rx.BehaviorSubject.create()

  app.hub:dump('hub', require('json').encode)  -- debug

  box.spacer = require 'spacer'({
      migrations = app.config.migrations,
      automigrate = app.config.automigrate
  })
  require 'schema'

  -- Load services
  local on_service_error
  local init_service
  local subscribtions = {}
  function init_service(name, serv)
    log.info(string.format('Starting "%s" service.', name))
    local source = rx.Subject.create()
    local source_sub = app.hub:subscribe(source)
    local sink = serv(
      app.config,
      source:delay(0, app.scheduler),
      app.scheduler
    )
    local sink_sub = nil
    if sink then
      sink_sub = sink
        :catch(util.partial(on_service_error, name, serv))
        :subscribe(app.hub)
    end
    subscribtions[name] = {source_sub, sink_sub}
    source:onNext({ topic = 'setup' })
  end
  function on_service_error(name, serv, err)
    for _m, sub in pairs(subscribtions[name]) do
      sub:unsubscribe()
    end
    subscribtions[name] = nil
    log.error('Error in service "' .. name .. '": ' .. err)
    fiber.sleep(1)
    init_service(name, serv)
    return true
  end
  local serv
  for _, name in ipairs(app.config.services) do
    serv = require('services.' .. name).service
    if serv then
      init_service(name, serv)
    end
  end
  app.scheduler:wait_idle()
  log.info('Application is ready')
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
