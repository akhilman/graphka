local api = require 'api'
local fiber = require 'fiber'

local M = {}


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

function M.service(config, source, scheduler)
  return api.publish(methods, 'test', 'api', source)
end


return M
