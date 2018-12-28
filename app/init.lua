local fiber = require 'fiber'
local conf = require 'config'
local log = require 'log'
local rx = require 'rx'
local rxtnt = require 'rxtnt'
local util = require 'util'

local default_config = {
  migrations = './migrations',
  timeout = 10,
  task_lifetime = 600,
  messages_per_task = 10000,
  purge_interval = 600,
  purge_message_limit = 100000,
  enable_test_service = false,
}

local services = {
  'api',
  'session',
  'node',
  'message',
  'task'
}

local app = {
  config = {},
}

function app.init(config)
  log.info('app "graphka" init')

  app.config = util.merge_tables(default_config, config)
  app.scheduler = rxtnt.FiberScheduler.create()
  app.hub = rx.BehaviorSubject.create()

  app.hub
    :filter(util.itemeq('topic', 'stop'))
    :subscribe(function() app.scheduler:stop() end)

  box.spacer = require 'spacer'({
      migrations = app.config.migrations,
      automigrate = app.config.automigrate
  })
  require 'schema'

  -- Exit on fault
  local function on_error(message)
    log.error(message)
    app.exit()
  end
  app.hub:subscribe(rx.util.noop, on_error, rx.util.noop)

  -- Debug
  if box.cfg.log_level >= 7 then
    app.hub:dump('hub', require('json').encode)  -- debug
  end

  -- Load services
  local to_start
  to_start = services
  if app.config.enable_test_service then
    table.insert(to_start, 'test')
  end
  local function init_service(name, serv)
    log.info(string.format('Starting "%s" service.', name))
    local source = rx.Subject.create()
    local sink = serv(app.config, source, app.scheduler)
    if sink then
      sink:subscribe(app.hub)
    end
    app.hub:subscribe(source)
    source:onNext({ topic = 'setup' })
  end
  local serv
  for _, name in ipairs(to_start) do
    serv = require('services.' .. name).service
    if serv then
      init_service(name, serv)
    end
  end

  -- Purge
  local trigger = rxtnt.interval(
      app.config.purge_interval * 1000, app.scheduler)
  trigger
    :map(function(n) return { topic = 'purge' } end)
    :subscribe(app.hub)

  log.info('Application is ready')

end

function app.destroy()
  log.info('Destroying application')
  app.hub:onNext({topic='stop'})
  fiber.sleep(0.3)
  app.hub:onCompleted()
end

function app.exit()
  app.destroy()
  os.exit()
end

if package.reload then
  package.reload:register(app)
end
rawset(_G, 'app', app)
return app
