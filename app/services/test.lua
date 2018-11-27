api = require 'api'
fiber = require 'fiber'

local services = {}
local methods = {}

function methods.echo(...)
  return {...}
end

function methods.delay(delay)
  delay = type(delay) == 'number' and delay or 10
  fiber.sleep(delay)
  return 'OK'
end

function methods.error(err)
  error {err}
end

function services.echo(config, source, scheduler)
  return api.publish(methods, 'test', 'api', source)
end

return {
  services = services
}
