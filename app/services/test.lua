local api = require 'api'
local fiber = require 'fiber'
local util = require 'util'

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
  if not err then err = 'Error in method' end
  error(err)
end

function methods.unprotected_error(err)
  if not err then err = 'Error in method' end
  return 'unprotected_error'
end

function M.service(config, source, scheduler)

  local sink = rx.Subject.create()

  api.publish(methods, 'test', 'app', source):subscribe(sink)

  source
    :filter(util.itemeq('topic', 'test_call'))
    :filter(util.itemeq('method', 'unprotected_error'))
    :map(util.itemgetter('args'))
    :map(rx.util.unpack)
    :map(function(txt) print('error', txt);  error(txt) end)
    :subscribe(sink)

  return sink
end


return M
