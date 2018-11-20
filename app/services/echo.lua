reqrep = require 'reqrep'

local services = {}
local methods = {}

function methods.echo(...)
  return {...}
end

function methods.error(err)
  error {err}
end

function services.echo(config, source)
  return reqrep.dispatch(source, 'echo', methods)
end

return {
  services = services
}
