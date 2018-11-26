api = require 'api'

local services = {}
local methods = {}

function methods.echo(...)
  return {...}
end

function methods.error(err)
  error {err}
end

function services.echo(config, source, scheduler)
  return api.publish(methods, 'echo', 'api', source)
end

return {
  services = services
}
