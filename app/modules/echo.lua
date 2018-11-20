reqrep = require 'reqrep'

local modules = {}
local methods = {}

function methods.echo(...)
  return {...}
end

function methods.error(err)
  error {err}
end

function modules.echo(config, source)
  return reqrep.dispatch(source, 'echo', methods)
end

return {
  modules = modules
}
